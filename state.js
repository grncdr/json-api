import {clone, patch as applyPatch} from 'jiff'
import xtend from 'xtend'
import from2 from 'from2'

export default function empty () {
  let log = []
  let state = {}
  let waiters = []

  return { apply, get, ops, size, changes }

  function apply (patch) {
    const invertiblePatch = makeInvertible(patch, state)
    const newState = applyPatch(invertiblePatch, state)
    state = newState
    log = log.concat(invertiblePatch)
    let waiters_ = waiters.slice()
    waiters = []
    waiters_.forEach(fn => fn())
    return {v: log.length};
  }

  function wait (notify) {
    waiters.push(notify)
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

  function size () {
    return log.length
  }

  function ops (from = 0, to = Infinity) {
    return log.slice(from, to)
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
    return from2(pull)

    function pull (size, next) {
      let chunk = ""
      size = size || 1024
      while (chunk.length < size && offset < state.size()) {
        state.ops(offset, offset + 10).forEach(op => {
          if (op.op === 'test' || op.path.substr(prefix.length) != prefix) {
            return
          }
          chunk += JSON.stringify(op) + '\n'
          offset++
        })
      }

      if (chunk) {
        chunk += `{"v":${subscriptionOffset}}\n`
        next(null, chunk)
      } else {
        wait(() => pull(size, next))
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
