const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: true }, maxHttpBufferSize: 1e6 });
const rooms = new Map();
const peers = new Map();

app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (_req, res) => res.json({ ok: true, service: 'LinguaRoom' }));

io.on('connection', socket => {
  socket.on('join-room', ({ room, role } = {}) => {
    if (typeof room !== 'string' || !/^[a-zA-Z0-9_-]{1,80}$/.test(room)) return;
    socket.data.room = room;
    socket.data.role = role === 'student' ? 'student' : 'teacher';
    socket.join(room);
    const members = rooms.get(room) || new Set();
    const existing = [...members].filter(id => id !== socket.id);
    members.add(socket.id);
    rooms.set(room, members);
    socket.emit('room-state', { count: members.size });
    existing.forEach(id => { peers.set(socket.id, id); peers.set(id, socket.id); io.to(id).emit('peer-joined', { id: socket.id, role: socket.data.role }); });
    existing.forEach(id => socket.emit('peer-joined', { id, role: 'peer' }));
  });
  socket.on('signal', ({ room, to, data } = {}) => {
    if (room !== socket.data.room || typeof to !== 'string' || !data || peers.get(socket.id) !== to) return;
    io.to(to).emit('signal', { from: socket.id, data });
  });
  for (const event of ['chat', 'homework', 'board-item', 'board-clear', 'sync', 'notes-sync']) {
    socket.on(event, payload => {
      if (!payload || payload.room !== socket.data.room) return;
      if (event === 'chat' && (typeof payload.text !== 'string' || payload.text.length > 4000)) return;
      if (event === 'board-item' && !payload.item) return;
      socket.to(payload.room).emit(event === 'board-item' ? 'board-item' : event, event === 'board-item' ? payload.item : payload);
    });
  }
  socket.on('leave-room', ({ room } = {}) => {
    if (room && room === socket.data.room) socket.leave(room);
  });
  socket.on('disconnect', () => {
    const room = socket.data.room;
    const peer = peers.get(socket.id);
    if (peer) { peers.delete(peer); peers.delete(socket.id); }
    if (!room) return;
    const members = rooms.get(room);
    if (members) {
      members.delete(socket.id);
      if (!members.size) rooms.delete(room);
    }
    socket.to(room).emit('peer-left', { id: socket.id });
  });
});

const port = process.env.PORT || 3000;
server.listen(port, '0.0.0.0', () => console.log(`LinguaRoom listening on ${port}`));
