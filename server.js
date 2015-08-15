/* @flow */
import createApp from 'web-pockets'
import xtend from 'xtend'

import initState from './state'
import openLog from './log'

const app = createApp()

export default app

app.value('states', new Map())

app.request.value('stateId', (request) => request.url.split('/')[1])

app.request.value('state', async (states, stateId) => {
  let state = states.get(stateId)
  if (!state) {
    state = initState(await openLog(__dirname + '/logs/' + stateId + '.json.log'))
    states.set(stateId, state)
  }
  return state
})

app.request.value('path', (request) => '/' + request.url.split('/').slice(2).join('/'))

app.request.value('patch', (path, parsedBody) => parsedBody.map(
  op => xtend(op, {path: path + op.path})
))

app.request.value('subscriptionOffset', (request) => {
  var n = parseInt(request.headers['x-subscription-offset'] || 0)
  if (isNaN(n) || n < 0) {
    throw new Error('x-subscription-offset must be a non-negative integer')
  }
  return n
})

app.request.wrap('result', async (getResult) => {
  const result = await getResult()
  if (result.error) {
    console.error(result.error.stack)
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

app.route('SUBSCRIBE /*', (state, path, subscriptionOffset) => ({
  headers: { 'Content-Type': 'application/vnd.patch_stream+json' },
  body: state.changes(path, subscriptionOffset)
}))

if (!module.parent) {
  app.listen(8080)
}
