/*
 * This class routes mqtt messages to local subscribers.
 *
 * It's interface is almost identical to the Paho MQTT client,
 * except that (un)subscriptions have an additional callback parameter.
 * If a message arrives it is propagated to the subscribed callback functions for that topic.
 *
 * It reconnects and resubscribes automatically on connection failures.
 * The reconnection mechanism is more configurable than the reconnection mechanism in the Paho MQTT client >= 1.0.4
 * It offers the following 2 additional parameters:
 * - initialRetryDelayMs: the initial delay before retrying to connect after an error
 * - maxRetryDelayMs: the maximum delay before retrying to connect after an error
 *
 * The translation of MQTT memessages into subscriber callbacks can be customized by means of onMessageArrived.
 *
 * The connect method returns a promise iso failure and success callbacks.
 */
class MQTTMux {

    constructor(id, url) {
        this.id = id;
        this.url = url;
        this.subscriptions = {};
    }

    /**
     * Default implementation. Just send the message.
     */
    onMessageArrived(msg, callback) {
        callback(msg);
    }

    // TODO consider using generator
    _resetRetryDelay() {
        this.retryDelayMs = this.initialRetryDelayMs;
    }

    _incrementRetryDelay() {
        if (this.retryDelayMs < this.maxRetryDelayMs) this.retryDelayMs *= 2;
    }

    subscribe(topic, callback, context) {
        const subscriptions = this.subscriptions[topic];
        this.subscriptions[topic] = (subscriptions || {RE: this._topicRE(topic), callbacks: []});
        this.subscriptions[topic].callbacks.push({callback, context});
        try {
            this.client.subscribe(topic);
        }
        catch(e) {
            // intentionally ignore undefined client and client not connected errors
        }
    }

    unsubscribe(topic, callback) {
        try {
            client.unsubscribe(topic);
        }
        catch(e) {
            // intentionally ignore undefined client and client not connected errors
        }
        const subscriptions = this.subscriptions[topic];
        const callbacks = subscriptions.callbacks.filter(s => s.callback !== callback);
        if (callbacks.length)
            subscriptions.callbacks = callbacks;
        else
            delete this.subscriptions[topic];
    }

    /* Turn topic into RegExp to match incoming messages efficiently */
    _topicRE(topic) {
        return new RegExp(topic.replace('+', '[^/]+').replace('#', '.*'));
    }

    connect(options) {
        this.initialRetryDelayMs = options.initialRetryDelayMs;
        this.maxRetryDelayMs = options.maxRetryDelayMs;
        this.connectOptions = _.omit(options, 'initialRetryDelayMs','maxRetryDelayMs');
        this._resetRetryDelay();
        return this._connect();
    }

    _connect() {
        return Promise.resolve(_.result(this, 'url'))
            .then(
                url => {
                    this.client = new Paho.MQTT.Client(url, _.result(this, 'id'));

                    this.client.onMessageArrived = msg => {
                        try {
                            msg.json = JSON.parse(msg.payloadString);
                            const topic = msg.destinationName;
                            Object.values(this.subscriptions).forEach(({RE, callbacks}) => {
                                if (RE.test(topic)) {
                                    callbacks.forEach(({callback, context}) =>
                                        this.onMessageArrived(msg, (...args) => {
                                            try {
                                                callback.call(context, ...args);
                                            }
                                            catch (e) {
                                                /* silently ignore */
                                            }
                                        })
                                    )
                                }
                            })
                        }
                        catch (err) {
                            /* silently ignore */
                        }
                    };

                    this.client.onConnectionLost = res => {
                        console.log(`MQTT Connection lost (${res.errorMessage}), reconnecting`);
                        this._connect();
                    };
                }
            )
            .then(() =>
                new Promise((onSuccess, onFailure) =>
                    this.client.connect(_.extend({onSuccess, onFailure}, this.connectOptions))
                )
                .then(
                    () => {
                        console.log("MQTT Connected");
                        this._resetRetryDelay();
                        for (const topic in this.subscriptions) this.client.subscribe(topic);
                    },
                    err => {
                        console.log(`MQTT error ${JSON.stringify(err)}`);
                        return new Promise(resolve => {
                                setTimeout(() => resolve(this._connect()), this.retryDelayMs);
                                this._incrementRetryDelay();
                            }
                        )
                    }
                )
            )
    }
}