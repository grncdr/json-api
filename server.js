/* @flow */
import createApp from 'web-pockets'
import xtend from 'xtend'
import {InvalidPatchOperationError, TestFailedError} from 'jiff'

import initState from './state'
import openLog from './server-log'

const app = createApp()

export default app

app.value('states', new Map())

app.request.value('stateId', (parsedUrl) => parsedUrl.pathname.split('/')[1])

app.request.value('state', async (states, stateId) => {
  let state = states.get(stateId)
  if (!state) {
    state = initState(await openLog(__dirname + '/logs/' + stateId + '.json.log'))
    states.set(stateId, state)
  }
  return state
})

app.request.value('path', (parsedUrl) => parsedUrl.pathname.split('/').slice(2).join('/'))

app.request.value('patch', (path, parsedBody) => parsedBody.map(
  op => xtend(op, {path: path + op.path})
))

app.request.value('changeStreamOptions', (queryParams) => {
  return {
    offset: intDefault(queryParams.offset, 0),
    timeout: Math.max(10000, intDefault(queryParams.timeout, 10000))
  }
})

function intDefault (s, defaultValue) {
  var n = parseInt(s || defaultValue)
  if (isNaN(n) || n < 0) {
    throw new Error('must be a non-negative integer')
  }
  return n
}

app.request.wrap('result', async (getResult) => {
  const result = await getResult()
  if (result.error) {
    if (result.error instanceof TestFailedError) {
      result.statusCode = 409
      result.body = { name: result.error.name, message: result.error.message }
    }
    else if (result.error instanceof InvalidPatchOperationError) {
      result.statusCode = 422
      result.body = { name: result.error.name, message: result.error.message }
    }
    else {
      console.error(result.error.stack)
    }
  }
  return result
})

app.routes({
  'PUT /*':    (state, path, parsedBody) => state.apply([{ op: 'add', path, value: parsedBody }]),
  'DELETE /*': (state, path)             => state.apply([{ op: 'remove', path }]),
  'PATCH /*':  (state, path, patch)      => state.apply(patch)
})

app.route('GET /*', async (state, path) => {
  const value = await state.get(path)
  // Return non-null objects directly
  if (value && typeof value == 'object') {
    return value
  }
  // JSON stringify primitives
  return {
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(value)
  }
})

app.route('SUBSCRIBE /*', (state, path, changeStreamOptions) => ({
  headers: { 'Content-Type': 'application/vnd.patch_stream+json' },
  body: state.changes(path, changeStreamOptions)
}))

if (!module.parent) {
  app.listen(8080)
}
