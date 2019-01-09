'use strict'

const setImmediate = require('async/setImmediate')
const NOT_STARTED_YET = require('./error-messages').NOT_STARTED_YET
const Pulsarcast = require('pulsarcast')

module.exports = (node) => {
  const pulsarcast = new Pulsarcast(node)

  node._pulsarcast = pulsarcast

  return {
    subscribe: (topic, options, handler, callback) => {
      if (typeof options === 'function') {
        callback = handler
        handler = options
        options = {}
      }

      if (!node.isStarted() && !pulsarcast.started) {
        return setImmediate(() => callback(new Error(NOT_STARTED_YET)))
      }

      function subscribe (cb) {
        if (pulsarcast.listenerCount(topic) === 0) {
          pulsarcast.subscribe(topic)
        }

        pulsarcast.on(topic, handler)
        setImmediate(cb)
      }

      subscribe(callback)
    },

    createTopic: (topic, options, handler, callback) => {
      if (typeof options === 'function') {
        callback = handler
        handler = options
        options = {}
      }

      if (!node.isStarted() && !pulsarcast.started) {
        return setImmediate(() => callback(new Error(NOT_STARTED_YET)))
      }

      function create (cb) {
        pulsarcast.createTopic(topic)
        pulsarcast.on(topic, handler)
        setImmediate(cb)
      }

      create(callback)
    },

    unsubscribe: (topic, handler) => {
      if (!node.isStarted() && !pulsarcast.started) {
        throw new Error(NOT_STARTED_YET)
      }

      pulsarcast.removeListener(topic, handler)

      if (pulsarcast.listenerCount(topic) === 0) {
        pulsarcast.unsubscribe(topic)
      }
    },

    publish: (topic, data, callback) => {
      if (!node.isStarted() && !pulsarcast.started) {
        return setImmediate(() => callback(new Error(NOT_STARTED_YET)))
      }

      if (!Buffer.isBuffer(data)) {
        return setImmediate(() => callback(new Error('data must be a Buffer')))
      }

      pulsarcast.publish(topic, data)

      setImmediate(() => callback())
    },

    ls: (callback) => {
      if (!node.isStarted() && !pulsarcast.started) {
        return setImmediate(() => callback(new Error(NOT_STARTED_YET)))
      }

      const subscriptions = Array.from(pulsarcast.subscriptions)

      setImmediate(() => callback(null, subscriptions))
    },

    peers: (topic, callback) => {
      if (!node.isStarted() && !pulsarcast.started) {
        return setImmediate(() => callback(new Error(NOT_STARTED_YET)))
      }

      if (typeof topic === 'function') {
        callback = topic
        topic = null
      }

      const peers = Array.from(pulsarcast.peers.values())
        .filter((peer) => topic ? peer.topics.has(topic) : true)
        .map((peer) => peer.info.id.toB58String())

      setImmediate(() => callback(null, peers))
    },

    setMaxListeners (n) {
      return pulsarcast.setMaxListeners(n)
    }
  }
}
