import * as fs from 'fs'
import split from 'split2'
import through from 'through2'

const cache = new Map()

export default async function getLog (file, idleTimeout = 10) {
  let log = cache.get(file)
  if (!log) {
    log = await open(file, idleTimeout, () => cache.delete(file))
    cache.set(file, log)
  }
  return log
}

async function open (file, idleTimeout, onClose) {
  const readers = []

  let writeStream;

  var timeout
  var currentWrite = Promise.resolve()

  return { write, read }

  function touch () {
    timeout && clearTimeout(timeout)
    timeout = setTimeout(cleanup, idleTimeout)
  }

  function cleanup () {
    writeStream && writeStream.end()
    writeStream = null
    readers.splice(0, readers.length).forEach(stream => stream.end())
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

    touch()

    await p(cb => writeStream.write(chunk, cb))
    let removedReaders = 0
    readers.forEach((stream, i) => {
      if (patch.some(op => !stream.write(op))) {
        // drop slow readers, they should reconnect later
        stream.end()
        readers.splice(i - removedReaders, 1)
        removedReaders++
      }
    })
  }

  function read (offset, follow = true) {
    touch()
    const source = fs.createReadStream(file)
    const lines = split()

    let counter = 0

    const opts = {highWaterMark: 1}
    const drop = through.obj(opts, (line, _, cb) => (++counter > offset) ? cb(null, line) : cb())
    const parse = through.obj(opts, (line, _, cb) => cb(null, JSON.parse(line)))
    const output = through.obj(opts)

    source.pipe(lines).pipe(drop).pipe(parse).pipe(output, {end: !follow})

    // forward all stream errors to the output stream
    ;[source, lines, drop, parse].forEach(
      s => s.on('error', e => output.emit('error', e))
    )

    // buffer any writes that occur while we are reading from disk
    const catchupBuffer = through.obj({ highWaterMark: 250 })
    readers.push(catchupBuffer)

    // once caught up from disk, flush any buffered writes
    parse.on('end', () => {
      parse.unpipe(output)
      catchupBuffer.pipe(output, {end: false})
    })

    catchupBuffer.on('end', () => {
      catchupBuffer.unpipe(output)

      const idx = readers.indexOf(catchupBuffer)

      if (idx < 0) {
        // The catchup buffer will be removed from the readers array for one of
        // two reasons:
        //
        //  1. The timeout for this log triggered cleanup, removing all readers
        //  2. This reader was too slow, and concurrent `write` calls during
        //     catchup maxed out the catchupBuffer and we were dropped.
        output.end()
      } else if (follow) {
        // replace catchupBuffer with the output
        readers[idx] = output
      } else {
        // stop forwarding writes to the catchupBuffer
        readers.splice(idx, 1)
      }
    })

    return output
  }
}

function p (fn) {
  return new Promise((resolve, reject) => fn((err, result) => err ? reject(err) : resolve(result)))
}
