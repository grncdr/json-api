import createApp from 'web-pockets'
import xtend from 'xtend'

import empty from './state'

const app = createApp()

export default app

app.value('states', new Map())

app.value('stateId', (request) => request.url.split('/')[1])

app.request.value('state', (states, stateId) => {
  if (!states.has(stateId)) {
    states.set(stateId, empty())
  }
  return states.get(stateId)
})

app.request.value('path', (request) => {
  return '/' + (request.url || '/').split('/').slice(2).join('/')
})

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

app.routes({
  'GET /*':    (state, path)             => state.get(path),
  'PUT /*':    (state, path, parsedBody) => state.apply([{ op: 'add', path, value: parsedBody }]),
  'DELETE /*': (state, path)             => state.apply({ op: 'remove', path }),
  'PATCH /*':  (state, path, patch)      => state.apply(patch)
})

app.route('SUBSCRIBE /*', (state, path, subscriptionOffset) => {
  return {
    headers: { 'Content-Type': 'application/vnd.patch_stream+json' },
    body: state.changes(path, subscriptionOffset)
  }
})

if (!module.parent) {
  app.listen(8080)
}
