import { createServer } from 'node:net';
import type { Socket } from 'node:net';
// Minimal local SMTP test fixture. CI runs the same tests against actual Mailpit.
// This fixture is never imported by the application.
export async function smtpFixture() {
  const messages: string[] = [];
  const sockets = new Set<Socket>();
  const server = createServer(socket => {
    sockets.add(socket); socket.on('error', () => undefined); socket.on('close', () => sockets.delete(socket));
    socket.setEncoding('utf8'); socket.write('220 local-test SMTP\r\n');
    let pending = ''; let body: string[] | null = null;
    socket.on('data', (data: string) => {
      pending += data;
      let end: number;
      while ((end = pending.indexOf('\r\n')) >= 0) {
        const line = pending.slice(0, end); pending = pending.slice(end + 2);
        if (body !== null) {
          if (line === '.') { messages.push(body.join('\r\n')); body = null; socket.write('250 accepted\r\n'); }
          else body.push(line.startsWith('..') ? line.slice(1) : line);
        } else if (/^EHLO|^HELO/.test(line)) socket.write('250-local-test\r\n250 8BITMIME\r\n');
        else if (line === 'DATA') { body = []; socket.write('354 send data\r\n'); }
        else if (line === 'QUIT') socket.end('221 bye\r\n');
        else socket.write('250 ok\r\n');
      }
    });
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('No SMTP TCP port');
  return { port: address.port, messages,
    close: async () => { for (const socket of sockets) socket.destroy(); await new Promise<void>(resolve => server.close(() => resolve())); } };
}
