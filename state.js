import {clone, patch as applyPatch} from 'jiff'
import through from 'through2'
import xtend from 'xtend'

export default async function fromLog (log) {
  let opCount = 0

  let state = await new Promise((resolve, reject) => {
    const patch = []
    log.read({offset: 0, follow: false})
      .on('data', op => patch.push(op))
      .on('error', reject).on('end', () => {
        opCount = patch.length
        resolve(applyPatch(compact(patch), {}))
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

  function changes ({prefix = '/', offset = 0, idleTimeout = 5000}) {
    const lines = log.read({offset, idleTimeout, follow: true})
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

    return lines.on('error', (e) => filter.emit('error', e)).pipe(filter)
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

/**
 * Compact a large patch by removing operations that are overwritten by later
 * ones.
 *
 * WARNING: only safe for creating a state snapshot from a simplified log!
 *
 * Algorithm description:
 */
function compact (patch) {
  let i = patch.length;
  let writtenPaths = []
  let newPatch = []

  nextOp: while (i > 0) {
    let op = patch[--i]
    // since log was already persisted, we can assume all tests passed.
    if (op.op === 'test') {
      continue;
    }

    for (let j = 0; j < writtenPaths.length; j++) {
      let writtenPath = writtenPaths[j]
      // we've already written to this path or an ancestor
      let writtenPathLength
      if (op.path.length >= writtenPath.length &&
          op.path.substr(0, writtenPath.length) === writtenPath)
      {
        continue nextOp;
      }
    }

    writtenPaths.push(op.path)
    newPatch.push(op)
  }

  return newPatch.reverse()
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
