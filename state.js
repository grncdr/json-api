import {clone, patch as applyPatch} from 'jiff'
import from2 from 'from2'
import through from 'through2'
import xtend from 'xtend'

import openLog from './log'

export default async function open (log) {
  let opCount = 0

  let state = await new Promise((resolve, reject) => {
    const patch = []
    // Maybe TODO - compact log operations before applying? The simplified
    // operations stored in the log will often overwrite pre-existing keys and
    // could therefore be skipped here. This would make loading O(n**2) but
    // patching O(m), m < n. Needs benchmarks first
    log.read(0, false).on('data', op => patch.push(op)).on('error', reject).on('end', () => {
      opCount = patch.length
      resolve(applyPatch(patch, {}))
    })
  })

  const waiting = []

  return { apply, get, changes }

  async function apply (patch) {
    const invertiblePatch = makeInvertible(patch, state)
    const newState = applyPatch(invertiblePatch, state)

    await log.write(invertiblePatch)
    opCount += invertiblePatch.length
    state = newState
    return {v: opCount};
  }

  function get (path) {
    if (path[0] !== '/') {
      throw new NotFound('Path must begin with a "/"')
    }
    const parts = path === '/' ? [] : path.split('/').slice(1) 
    var value = parts.reduce((o, key) => {
      if (!o || typeof o !== 'object' || !(key in o)) {
        throw new NotFound(path, key)
      }
      return o[key]
    }, state)
    return clone(value)
  }

  /**
   * Ensure that we only have invertible operations in our patch
   */
  function makeInvertible (patch) {
    return patch.reduce((patch, op) => {
      if (op.op === 'copy') {
        return patch.concat(simplify({ op: 'add', path: op.path, value: get(op.from) }))
      }
      if (op.op === 'remove' || op.op === 'replace') {
        patch.push({ op: 'test', path: op.path, value: get(op.path) })
      }
      return patch.concat(simplify(op))
    }, [])
  }

  function changes (prefix, offset = 0) {
    const lines = log.read(offset)
    const filter = through.obj({highWaterMark: 1}, (op, _, next) => {
      offset++
      if (op.path.substr(0, prefix.length) === prefix) {
        next(null, JSON.stringify(op) + '\n')
      } else {
        next()
      }
    }, (next) => {
      filter.push(`{"v":${offset}}\n`)
      next()
    })

    return lines.pipe(filter)

    return from2(pull)

    function pull (size, next) {
      let chunk = ""
      size = size || 1024

      while (chunk.length < size && offset < log.length) {
        let op = log[offset++]
        if (op.op !== 'test' || op.path.substr(prefix.length) == prefix) {
          chunk += JSON.stringify(op) + '\n'
        }
      }

      if (chunk) {
        chunk += `{"v":${offset}}\n`
        next(null, chunk)
      } else {
        waiting.push(() => pull(size, next))
      }
    }
  }
}

function simplify (op) {
  if (!op.value || typeof op.value !== 'object') {
    return [op]
  }
  if (Array.isArray(op.value)) {
    return [xtend(op, {value: []})].concat(
      op.value.map((value, i) => simplify(xtend(op, {value, path: op.path + '/' + i})))
    )
  }
  let patch = [xtend(op, {value: {}})]
  for (let k in op.value) {
    patch = patch.concat(simplify(xtend(op, {value: op.value[k], path: op.path + '/' + k})))
  }
  return patch
}

class NotFound extends Error {
  name = 'NotFound'
  statusCode = 404

  constructor (path, key) {
    super()
    this.message = `Key '${key}' in path '${path}' does not exist.`
  }

  toJSON () {
    return { statusCode: this.statusCode, body: { name: this.name, message: this.message } }
  }
}
