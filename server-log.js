import * as fs from 'fs'
import split from 'split2'
import through from 'through2'

const cache = new Map()

/**
 * getLog (file, { writeTimeout = 10000, readTimeout = 10000 }): { read, write }
 *
 * Returns an object which has two methods:
 *
 *   async write(patch)
 *
 *     Takes an array of operations (a JSON patch) and appends it to the log.
 *
 *   read(opts): Readable
 *
 *     Return a readable object stream of operations beginning at `offset`. If
 *     `follow` is true (the default), the stream will remain open upon reaching
 *     the end of the log and receive future writes as long as the log file
 *     itself is open.
 *
 */
export default async function getLog (file, idleTimeout = 10000) {
  let log = cache.get(file)
  if (!log) {
    log = await open(file, idleTimeout, () => cache.delete(file))
    cache.set(file, log)
  }
  return log
}

async function open (file, writeIdleTimeout, onClose) {
  const tails = []

  let writeStream;

  var writeTimeout
  var currentWrite = Promise.resolve()

  return { write, read }

  function cleanup () {
    writeStream && writeStream.end()
    writeStream = null
    onClose()
  }

  async function write (patch) {
    const chunk = patch.reduce((chunk, op) => `${chunk}${JSON.stringify(op)}\n`, '')
    if (!chunk) {
      return
    }

    if (!writeStream) {
      writeStream = fs.createWriteStream(file, {flags: 'a'})
    }

    // Reset the idle timeout
    writeTimeout && clearTimeout(writeTimeout)
    writeTimeout = setTimeout(cleanup, writeIdleTimeout)

    await p(cb => writeStream.write(chunk, cb))
    let removedTails = 0
    tails.forEach((tail, i) => {
      if (!tail.write(patch)) {
        // drop slow tails
        tail.end()
        tails.splice(i - removedTails, 1)
        removedTails++
      }
    })
  }

  function read (opts) {
    const diskOps = readOpsFromDisk(file, opts.offset)

    if (opts.follow) {
      const { input, output } = createTail(diskOps, opts)
      output.on('end', () => {
        let idx = tails.indexOf(input)
        if (idx >= 0) {
          tails.splice(idx, 1)
        }
      })
      tails.push(input)
      return output
    } else {
      return diskOps
    }
  }
}

function readOpsFromDisk (file, offset) {
  const source = fs.createReadStream(file)
  const lines = split()

  let counter = 0

  const drop   = through.obj((line, _, cb) => (++counter > offset) ? cb(null, line) : cb())
  const parse  = through.obj((line, _, cb) => cb(null, JSON.parse(line)))

  // forward all stream errors to the output stream
  ;[source, lines, drop].forEach(
    s => s.on('error', e => parse.emit('error', e))
  )

  return source.pipe(lines).pipe(drop).pipe(parse)
}

function createTail (diskOps, {
  catchupBufferSize = 256,
  outputBufferSize = 128,
  idleTimeout = 250
}) {
  let buffer = null
  let liveStream = null
  let caughtUp = false
  let timeout = null
  let ended = false

  const output = through.obj({highWaterMark: outputBufferSize})
  const onTimeout = () => output.end()

  diskOps.pipe(output, {end: false})
  diskOps.on('end', () => {
    diskOps.unpipe(output)

    if (buffer) {
      buffer.pipe(output, {end: false})
      buffer.on('end', () => {
        buffer.unpipe(output)
        buffer = null
        caughtUp = true
        timeout = !ended && setTimeout(onTimeout, idleTimeout)
      })
    } else {
      caughtUp = true
      timeout = !ended && setTimeout(onTimeout, idleTimeout)
    }
  })

  function write (patch) {
    if (ended) { return false }

    let targetStream
    if (caughtUp) {
      targetStream = output
      clearTimeout(timeout)
      timeout = setTimeout(onTimeout, idleTimeout)
    } else {
      targetStream = buffer = (buffer || through.obj({highWaterMark: catchupBufferSize}))
    }

    if (targetStream.writable) {
      return patch.reduce((_, op) => targetStream.write(op))
    }
  }

  function end () {
    if (ended) { return }
    ended = true
    if (caughtUp) {
      clearTimeout(timeout)
      output.end()
    } else if (buffer) {
      buffer.end()
    }
  }

  return { input: { write, end }, output }
}

function p (fn) {
  return new Promise((resolve, reject) => fn((err, result) => err ? reject(err) : resolve(result)))
}
