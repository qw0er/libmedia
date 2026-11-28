/*
 * libmedia fetch loader
 *
 * 版权所有 (C) 2024 赵高兴
 * Copyright (C) 2024 Gaoxing Zhao
 *
 * 此文件是 libmedia 的一部分
 * This file is part of libmedia.
 * 
 * libmedia 是自由软件；您可以根据 GNU Lesser General Public License（GNU LGPL）3.1
 * 或任何其更新的版本条款重新分发或修改它
 * libmedia is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.1 of the License, or (at your option) any later version.
 * 
 * libmedia 希望能够为您提供帮助，但不提供任何明示或暗示的担保，包括但不限于适销性或特定用途的保证
 * 您应自行承担使用 libmedia 的风险，并且需要遵守 GNU Lesser General Public License 中的条款和条件。
 * libmedia is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 */

import { Sleep } from '@libmedia/common/timer'
import { errorType } from '@libmedia/avutil'
import { object, logger, getTimestamp } from '@libmedia/common'
import { IOError, type Uint8ArrayInterface } from '@libmedia/common/io'

import type { IOLoaderOptions } from './IOLoader'
import IOLoader, { IOLoaderStatus } from './IOLoader'
import type { Data, HttpOptions, Range } from '@libmedia/common'

export interface FetchInfo {
  url: string
  httpOptions?: HttpOptions
}

export interface FetchIOLoaderOptions extends IOLoaderOptions {
  disableSegment?: boolean
  enableBandwidthReader?: boolean
}

export default class FetchIOLoader extends IOLoader {

  declare public options: FetchIOLoaderOptions

  private contentLength: number

  private receivedLength: number

  private info: FetchInfo

  private range: Range

  private startBytes: number

  private endBytes: number

  private eofIndex: number

  private abortController: AbortController

  private reader: ReadableStreamDefaultReader<Uint8Array>

  private buffers: Uint8Array[]

  private supportRange: boolean

  private abortSleep_: Sleep

  private aborted: boolean

  private fetchCancelled = false

  private bandwidth: number

  private delay: number

  constructor(options: FetchIOLoaderOptions = {}) {
    super(options)
  }

  private async getTotalSize(method: string, headers: Data = {}) {
    if (this.fetchCancelled || this.aborted) {
      return 0
    }
    const maxAttempts = Math.min(2, this.options.retryCount) + 1
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      if (this.fetchCancelled || this.aborted) {
        return 0
      }
      const params: RequestInit = {
        method: method,
        headers: {},
        mode: 'cors',
        cache: 'default',
        referrerPolicy: 'no-referrer-when-downgrade'
      }
      if (this.info.httpOptions?.headers) {
        object.each(this.info.httpOptions.headers, (value, key) => {
          params.headers[key] = value
        })
      }

      object.each(headers, (value, key) => {
        params.headers[key] = value
      })

      if (this.info.httpOptions?.credentials) {
        params.credentials = this.info.httpOptions.credentials
      }

      if (this.info.httpOptions?.referrerPolicy) {
        params.referrerPolicy = this.info.httpOptions.referrerPolicy
      }
      if (typeof AbortController === 'function') {
        this.abortController = new AbortController()
        params.signal = this.abortController.signal
      }

      try {
        const res = await fetch(this.info.url, params)
        if (this.abortController) {
          this.abortController.abort()
          this.abortController = null
        }
        if (res.ok && (res.status >= 200 && res.status <= 299)) {
          const acceptRanges = res.headers.get('Accept-Ranges')
          if (acceptRanges) {
            if (acceptRanges.indexOf('bytes') === -1) {
              this.supportRange = false
            }
          }

          const contentRange = res.headers.get('Content-Range')
          if (contentRange) {
            const size = contentRange.split('/')[1]
            if (size) {
              return parseInt(size)
            }
          }
          const lengthHeader = res.headers.get('X-Content-Length') || res.headers.get('Content-Length')
          if (lengthHeader != null) {
            return parseInt(lengthHeader)
          }
        }
        return 0
      }
      catch (error) {
        if (this.fetchCancelled || this.aborted || attempt + 1 >= maxAttempts) {
          return 0
        }
        this.abortSleep_ = new Sleep(this.options.retryInterval)
        await this.abortSleep_
        this.abortSleep_ = null
      }
    }
    return 0
  }

  public async open(info: FetchInfo, range: Range = { from: 0, to: -1 }) {

    if (this.fetchCancelled) {
      return IOError.END
    }
    this.info = info
    this.range = range

    if (!this.range.to) {
      this.range.to = -1
    }

    this.range.from = Math.max(this.range.from, 0)

    this.contentLength = undefined
    this.startBytes = 0
    this.endBytes = -1
    this.eofIndex = -1
    this.receivedLength = 0
    this.retryCount = 0
    this.buffers = []
    this.supportRange = true
    this.aborted = false
    this.bandwidth = 0

    if (this.range.to > 0) {
      this.eofIndex = range.to
    }

    if (this.range && !this.options.isLive) {
      this.startBytes = this.range.from ?? 0
    }

    this.status = IOLoaderStatus.CONNECTING

    if (!this.options.isLive && !this.options.disableSegment) {
      let totalSize = await this.getTotalSize('HEAD')
      if (!totalSize) {
        totalSize = await this.getTotalSize('GET', {
          range: 'bytes=0-1'
        })
      }
      if (totalSize <= 2) {
        totalSize = 0
      }
      if (totalSize) {
        this.contentLength = totalSize
        if (this.range.to < 0) {
          this.eofIndex = this.contentLength + this.range.to
        }
        if (this.supportRange) {
          this.endBytes = Math.min(this.startBytes + this.options.preload - 1, this.eofIndex)
        }
      }
      else {
        this.endBytes = -1
      }
    }

    if (this.fetchCancelled) {
      return IOError.END
    }
    this.status = IOLoaderStatus.BUFFERING

    return 0
  }

  private getBandwidthReader(reader: ReadableStreamDefaultReader<Uint8Array<ArrayBufferLike>>, start: number) {
    const buffers: Uint8Array<ArrayBufferLike>[] = []
    let end = false
    let readError: unknown
    let pending: () => void
    let totals = 0
    const me = this
    const stream = new ReadableStream({
      async start(controller) {
        try {
          while (true) {
            const { value, done } = await reader.read()
            if (done) {
              end = true
              const duration = (getTimestamp() - start) / 1000
              me.bandwidth = totals * 8 / duration
              if (pending) {
                pending()
                pending = null
              }
              break
            }
            else {
              buffers.push(value)
              totals += value.length
              if (pending) {
                pending()
                pending = null
              }
            }
          }
        }
        catch (error) {
          readError = error
          end = true
          if (pending) {
            pending()
            pending = null
          }
        }
      },
      async pull(controller) {
        while (true) {
          if (buffers.length) {
            controller.enqueue(buffers.shift())
            return
          }
          if (readError) {
            controller.error(readError)
            return
          }
          if (end && !buffers.length) {
            controller.close()
            return
          }
          await new Promise<void>((resolve) => {
            pending = resolve
          })
        }
      }
    })
    return stream.getReader()
  }

  private async openReader() {
    const params: RequestInit = {
      method: 'GET',
      headers: {},
      mode: 'cors',
      cache: 'default',
      referrerPolicy: 'no-referrer-when-downgrade'
    }
    if (this.info.httpOptions?.headers) {
      object.each(this.info.httpOptions.headers, (value, key) => {
        params.headers[key] = value
      })
    }
    if (!this.options.isLive && !this.options.disableSegment) {
      params.headers['range'] = `bytes=${this.startBytes}-${this.endBytes > 0 ? this.endBytes : ''}`
    }

    if (this.info.httpOptions?.credentials) {
      params.credentials = this.info.httpOptions.credentials
    }

    if (this.info.httpOptions?.referrerPolicy) {
      params.referrerPolicy = this.info.httpOptions.referrerPolicy
    }

    if (this.abortController) {
      this.abortController.abort()
    }

    if (typeof AbortController === 'function') {
      this.abortController = new AbortController()
      params.signal = this.abortController.signal
    }

    try {
      const start = getTimestamp()
      const res = await fetch(this.info.url, params)
      if (this.fetchCancelled || this.aborted) {
        await res.body?.cancel().catch(() => {})
        return
      }
      const openStart = getTimestamp()
      this.delay = openStart - start
      if (res.ok && (res.status >= 200 && res.status <= 299)) {
        if (this.startBytes > 0 && res.status !== 206) {
          await res.body?.cancel().catch(() => {})
          this.supportRange = false
          this.status = IOLoaderStatus.ERROR
          logger.error(`Media HTTP range request ignored, status: ${res.status}`)
          return
        }
        this.reader = this.options.enableBandwidthReader ? this.getBandwidthReader(res.body.getReader(), openStart) : res.body.getReader()
      }
      else {
        this.status = IOLoaderStatus.ERROR
        logger.error(`Media HTTP request failed, status: ${res.status}`)
      }
    }
    catch (error) {
      if (this.fetchCancelled || this.aborted) {
        return
      }
      if (this.retryCount < this.options.retryCount && (!this.options.isLive || !this.receivedLength)) {
        this.retryCount++
        this.status = IOLoaderStatus.CONNECTING

        this.abortSleep_ = new Sleep(this.options.retryInterval)
        await this.abortSleep_
        this.abortSleep_ = null
        if (this.aborted || this.fetchCancelled) {
          return
        }
        return this.openReader()
      }
      else {
        this.status = IOLoaderStatus.ERROR
        logger.error('Media fetch failed')
      }
    }
  }

  private canRetryStream() {
    if (this.fetchCancelled || this.aborted) {
      return false
    }
    if (this.retryCount >= this.options.retryCount) {
      return false
    }
    const currentOffset = this.range.from + this.receivedLength
    if (currentOffset === 0) {
      return true
    }
    return !this.options.isLive && !this.options.disableSegment && this.supportRange
  }

  private async resetReader() {
    if (this.reader) {
      await this.reader.cancel().catch(() => {})
      this.reader = null
    }
    if (this.abortController) {
      this.abortController.abort()
      this.abortController = null
    }
  }

  private async retryStreamRead(): Promise<boolean> {
    if (!this.canRetryStream()) {
      return false
    }
    await this.resetReader()
    if (this.fetchCancelled || this.aborted) {
      return false
    }
    this.retryCount++
    this.status = IOLoaderStatus.CONNECTING
    this.abortSleep_ = new Sleep(this.options.retryInterval)
    await this.abortSleep_
    this.abortSleep_ = null
    if (this.fetchCancelled || this.aborted) {
      return false
    }
    this.startBytes = this.range.from + this.receivedLength
    if (this.contentLength !== undefined && this.eofIndex >= 0 && this.supportRange) {
      this.endBytes = Math.min(this.startBytes + this.options.preload - 1, this.eofIndex)
    }
    else {
      this.endBytes = -1
    }
    this.status = IOLoaderStatus.BUFFERING
    return true
  }

  private async readInterval(buffer: Uint8ArrayInterface, preLen: int32 = 0): Promise<number> {

    if (this.fetchCancelled) {
      return IOError.END
    }
    let pos = 0

    while (this.buffers.length && pos < buffer.length) {
      const cache = this.buffers.shift()
      if (cache.length > buffer.length - pos) {
        buffer.set(cache.subarray(0, buffer.length - pos), pos)
        this.buffers.unshift(cache.subarray(buffer.length - pos))
        pos = buffer.length
      }
      else {
        buffer.set(cache, pos)
        pos += cache.length
      }
    }

    if (pos >= buffer.length) {
      return buffer.length + preLen
    }

    if (this.status === IOLoaderStatus.COMPLETE) {
      return pos > 0 ? (pos + preLen) : (preLen > 0 ? preLen : IOError.END)
    }

    if (!this.reader) {
      await this.openReader()
      if (this.aborted || this.fetchCancelled) {
        return IOError.END
      }
      if (!this.reader) {
        return IOError.NETWORK_ERROR
      }
    }

    let result: ReadableStreamReadResult<Uint8Array>
    try {
      result = await this.reader.read()
    }
    catch (error) {
      if (this.fetchCancelled || this.aborted) {
        return IOError.END
      }
      if (await this.retryStreamRead()) {
        return this.readInterval(buffer.subarray(pos, undefined, true), pos + preLen)
      }
      if (this.fetchCancelled || this.aborted) {
        return IOError.END
      }
      await this.resetReader()
      this.status = IOLoaderStatus.ERROR
      logger.error(`Fetch stream read failed: ${error}`)
      return IOError.NETWORK_ERROR
    }

    if (this.fetchCancelled || this.aborted) {
      return IOError.END
    }
    const { value, done } = result

    if (done) {
      const expectedEnd = this.endBytes >= 0 ? this.endBytes : this.eofIndex
      if (this.contentLength !== undefined && expectedEnd >= 0 && (this.receivedLength + this.range.from) < expectedEnd + 1) {
        if (await this.retryStreamRead()) {
          return this.readInterval(buffer.subarray(pos, undefined, true), pos + preLen)
        }
        if (this.fetchCancelled || this.aborted) {
          return IOError.END
        }
        await this.resetReader()
        this.status = IOLoaderStatus.ERROR
        logger.error('Fetch stream meet Early-EOF')
        return IOError.NETWORK_ERROR
      }
      else if (this.options.isLive || this.options.disableSegment || (this.receivedLength + this.range.from) >= this.eofIndex) {
        this.status = IOLoaderStatus.COMPLETE
        this.startBytes = 0
        return pos > 0 ? (pos + preLen) : (preLen > 0 ? preLen : IOError.END)
      }
      else {
        await this.resetReader()

        this.startBytes = this.endBytes + 1
        this.endBytes = Math.min(this.startBytes + this.options.preload - 1, this.eofIndex)

        return this.readInterval(buffer.subarray(pos, undefined, true), pos + preLen)
      }
    }
    else {
      if (!value || !value.length) {
        return this.readInterval(buffer.subarray(pos, undefined, true), pos + preLen)
      }

      this.retryCount = 0
      this.receivedLength += value.length

      if (value.length > buffer.length - pos) {
        buffer.set(value.subarray(0, buffer.length - pos), pos)
        this.buffers.push(value.subarray(buffer.length - pos))
        return buffer.length + preLen
      }
      else {
        buffer.set(value, pos)
        pos += value.length
        return pos + preLen
      }
    }
  }

  public async read(buffer: Uint8ArrayInterface): Promise<int32> {
    return this.readInterval(buffer)
  }

  public async seek(pos: int64) {
    if (this.fetchCancelled) {
      return IOError.END
    }

    if (!this.supportRange) {
      return errorType.OPERATE_NOT_SUPPORT
    }
    if (this.eofIndex > 0 && Number(pos) > this.eofIndex) {
      return errorType.INVALID_PARAMETERS
    }

    await this.abort()
    if (this.fetchCancelled) {
      return IOError.END
    }
    this.aborted = false
    this.receivedLength = Number(pos) - this.range.from
    this.startBytes = Number(pos)
    if (!this.options.disableSegment) {
      this.endBytes = Math.min(this.startBytes + this.options.preload, this.eofIndex)
    }
    this.buffers.length = 0
    if (this.status === IOLoaderStatus.COMPLETE) {
      this.status = IOLoaderStatus.BUFFERING
    }

    return 0
  }

  public async size() {
    if (this.options.isLive) {
      return 0n
    }
    return static_cast<int64>(this.contentLength || 0)
  }

  /** Permanently cancels this loader, including requests waiting for headers. */
  public cancelFetch() {
    this.fetchCancelled = true
    this.abortSleep()
    this.abortController?.abort()
  }

  public abortSleep() {
    this.aborted = true
    if (this.abortSleep_) {
      this.abortSleep_.stop()
      this.abortSleep_ = null
    }
  }

  public async abort() {
    this.abortSleep()
    if (!this.reader) {
      return
    }
    await this.reader.cancel()
    if (this.abortController) {
      this.abortController.abort()
      this.abortController = null
    }
    this.reader = null
  }

  public async stop() {
    this.status = IOLoaderStatus.IDLE
    await this.abort()
  }

  public getUrl() {
    return this.info?.url
  }
}
