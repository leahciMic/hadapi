import serializeError from 'serialize-error';
import zeromq from 'zeromq';
import nanoid from 'nanoid';

// Hadapi (Highly available distributed application programming interface)

export async function exposeRemoteProcedure(
  api,
  { connect, bind, socketType = 'rep', linger = 5000 } = {}
) {
  const replySocket = zeromq.socket(socketType);
  replySocket.setsockopt(zeromq.ZMQ_LINGER, linger);

  const validSocketTypes = ['rep', 'router', 'dealer'];

  if (!validSocketTypes.includes(socketType)) {
    throw new Error('invalid socket type');
  }

  if (typeof connect === 'string') {
    replySocket.connect(connect);
  } else if (typeof bind === 'string') {
    await new Promise((resolve, reject) => {
      replySocket.bind(bind, (err, value) => {
        if (err) {
          reject(err);
        } else {
          resolve(value);
        }
      });
    });
  } else {
    throw new Error('missing connect or bind paramter');
  }

  replySocket.on('message', async (...frames) => {
    let reply;
    let parsedData;

    if (socketType === 'rep' && frames.length === 1) {
      parsedData = JSON.parse(frames[0]);

      reply = params => {
        replySocket.send(
          JSON.stringify({
            ...params,
            messageId: parsedData.messageId
          })
        );
      };
    } else if (socketType === 'router' && frames.length === 3) {
      parsedData = JSON.parse(frames[2]);

      reply = params => {
        replySocket.send([
          frames[0],
          frames[1],
          JSON.stringify({
            ...params,
            messageId: parsedData.messageId
          })
        ]);
      };
    } else if (socketType === 'router' && frames.length > 3) {
      parsedData = JSON.parse(frames[frames.length - 1]);
      reply = params => {
        replySocket.send(
          frames
            .slice(0, frames.length - 1)
            .concat([JSON.stringify({ ...params, messageId: parsedData.messageID })])
        );
      };
    } else {
      throw new Error('unexpected condition (possibly not implemented)');
    }

    const { operation, messageId, ...parsed } = parsedData;

    try {
      if (typeof api[operation] !== 'function') {
        throw new Error('Operation does not exist');
      }

      const returnValue = await api[operation](parsed);

      reply({ operation: 'REPLY', returnValue });
    } catch (err) {
      reply({
        operation: 'EXCEPTION',
        exception: serializeError(err)
      });
    }
  });

  return replySocket;
}

// Send to othersied and receive information
// @todo have a requestId to ensure we only process things once
// and that replies are for the data we send...
export function consumeRemoteProcedure({ connect, bind, socketType = 'req', linger = 5000 } = {}) {
  const requestSocket = zeromq.socket(socketType);
  requestSocket.setsockopt(zeromq.ZMQ_LINGER, linger);

  const validSocketTypes = ['req', 'router', 'dealer'];

  if (!validSocketTypes.includes(socketType)) {
    throw new Error('invalid socket type');
  }

  if (typeof connect === 'string') {
    requestSocket.connect(connect);
  } else if (typeof bind === 'string') {
    requestSocket.bind(bind);
  } else {
    throw new Error('missing connect or bind paramter');
  }

  const replyMap = new Map();

  requestSocket.on('message', (...frames) => {
    let result;
    let messageId;

    if (socketType === 'req' && frames.length === 1) {
      result = JSON.parse(frames[0]);
      ({ messageId } = result);
    } else if (socketType === 'dealer' && frames.length === 3) {
      result = JSON.parse(frames[2]);
      messageId = frames[0].toString('utf8');
    } else {
      throw new Error('unexpected condition (possibly not implemented)');
    }

    if (!messageId) {
      throw new Error('Received message without an identifier');
    }

    if (!replyMap.has(messageId)) {
      throw new Error('Received unsolicited message');
    }
    const promise = replyMap.get(messageId);

    replyMap.delete(messageId);

    if (result.operation === 'EXCEPTION') {
      const err = new Error(result.exception.message);
      Object.assign(err, result.exception);
      promise.reject(err);
    } else if (result.operation === 'REPLY') {
      promise.resolve(result.returnValue);
    } else {
      throw new Error('unexpected condition (possibly not implemented)');
    }
  });

  const deferPromise = () => {
    let resolve;
    let reject;

    const promise = new Promise((tmpResolve, tmpReject) => {
      resolve = tmpResolve;
      reject = tmpReject;
    });

    return {
      promise,
      reject,
      resolve
    };
  };

  return new Proxy(
    {
      async close() {
        return requestSocket.close();
      },
      async unmonitor() {
        return requestSocket.unmonitor();
      },
      async disconnect(addr) {
        return requestSocket.disconnect(addr);
      }
    },
    {
      get(target, property) {
        if (target[property]) {
          return target[property];
        }
        return async namedArgs => {
          const messageId = nanoid();
          const deferred = deferPromise();
          replyMap.set(messageId, deferred);

          if (socketType === 'req') {
            requestSocket.send(
              JSON.stringify({
                messageId,
                operation: property,
                ...namedArgs
              })
            );
          } else if (socketType === 'dealer') {
            requestSocket.send([
              messageId,
              '',
              JSON.stringify({
                messageId,
                operation: property,
                ...namedArgs
              })
            ]);
          } else {
            throw new Error(`socket type ${socketType} not implemented yet`);
          }

          return deferred.promise;
        };
      }
    }
  );
}

export function createBrokerProxy({
  backendAddress = 'tcp://*:5000',
  frontendAddress = 'tcp://*:5001'
} = {}) {
  const frontend = zeromq.socket('router');
  const backend = zeromq.socket('dealer');

  frontend.bindSync(frontendAddress);
  backend.bindSync(backendAddress);

  zeromq.proxy(frontend, backend);
}
