/* eslint-env jest */
const hadapi = require('./index');

describe('Hadapi', () => {
  it('receive correct value from remote function', async () => {
    const server = await hadapi.exposeRemoteProcedure(
      {
        hello({ name }) {
          return `Hi ${name}`;
        }
      },
      {
        bind: 'inproc://a',
        linger: 0
      }
    );

    const api = hadapi.consumeRemoteProcedure({ connect: 'inproc://a', linger: 0 });

    // @todo expose an easy method for correct shutdown for both bind, and connect
    // scenarios
    await new Promise((resolve, reject) => {
      server.unbind('inproc://a', (err, value) => {
        if (err) {
          reject(err);
        } else {
          resolve(value);
        }
      });
    });

    await api.disconnect('inproc://a');
    await api.close();
    await server.close();
  });
});
