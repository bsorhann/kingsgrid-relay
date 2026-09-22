// ============================================================
// KINGSGRID — relay-server/server.js
// ============================================================
// Basit bir eşleştirme + mesaj aktarma (relay) sunucusu. HİÇBİR oyun
// mantığı BURADA yaşamaz — sadece iki oyuncuyu (oda koduyla ya da
// rastgele) eşleştirir, sonra aralarındaki HER mesajı (opak bir metin
// olarak) birbirine iletir. Godot tarafı (NetworkManager.gd) mesajların
// İÇERİĞİNİ (method/args/zarlar) kendi Variant serileştirmesiyle
// (var_to_str/str_to_var) kodlar — bu sunucu onu hiç AÇMAZ/YORUMLAMAZ.
//
// Çalıştırmak için: npm install ws && node server.js
// (Render, Railway, Fly.io gibi ücretsiz bir platforma da BÖYLECE
// deploy edilebilir — sadece PORT ortam değişkenini okur.)

const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// rooms: kod -> { host: ws, guest: ws|null }
const rooms = new Map();
// randomQueue: rastgele eşleşme bekleyen bağlantılar (FIFO).
let randomQueue = [];

const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // karışabilecek 0/O, 1/I çıkarıldı
const CODE_LENGTH = 5;
const ROOM_TIMEOUT_MS = 10 * 60 * 1000; // misafir gelmezse 10 dakika sonra oda otomatik silinir

function generateCode() {
	let code;
	do {
		code = '';
		for (let i = 0; i < CODE_LENGTH; i++) {
			code += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
		}
	} while (rooms.has(code));
	return code;
}

function send(ws, obj) {
	if (ws && ws.readyState === WebSocket.OPEN) {
		ws.send(JSON.stringify(obj));
	}
}

function cleanupConnection(ws) {
	randomQueue = randomQueue.filter((w) => w !== ws);
	if (ws.roomCode && rooms.has(ws.roomCode)) {
		const room = rooms.get(ws.roomCode);
		if (room.timeoutHandle) clearTimeout(room.timeoutHandle);
		rooms.delete(ws.roomCode);
	}
	if (ws.peer) {
		send(ws.peer, { type: 'peer_disconnected' });
		ws.peer.peer = null;
	}
}

function pair(peerA, peerB, roleAIsHost) {
	peerA.peer = peerB;
	peerB.peer = peerA;
	if (peerA.roomCode && rooms.has(peerA.roomCode)) {
		const room = rooms.get(peerA.roomCode);
		if (room.timeoutHandle) clearTimeout(room.timeoutHandle);
	}
	send(peerA, { type: 'paired', role: roleAIsHost ? 'host' : 'guest' });
	send(peerB, { type: 'paired', role: roleAIsHost ? 'guest' : 'host' });
}

wss.on('connection', (ws) => {
	ws.peer = null;
	ws.roomCode = null;

	ws.on('message', (raw) => {
		let msg;
		try {
			msg = JSON.parse(raw.toString());
		} catch (e) {
			return; // bozuk/JSON-olmayan mesajı sessizce yok say
		}

		switch (msg.type) {
			case 'create_room': {
				const code = generateCode();
				const timeoutHandle = setTimeout(() => {
					if (rooms.has(code) && !rooms.get(code).guest) {
						rooms.delete(code);
						send(ws, { type: 'error', message: 'Oda süresi doldu, kimse katılmadı.' });
					}
				}, ROOM_TIMEOUT_MS);
				rooms.set(code, { host: ws, guest: null, timeoutHandle });
				ws.roomCode = code;
				send(ws, { type: 'room_created', code });
				break;
			}
			case 'join_room': {
				const code = String(msg.code || '').toUpperCase();
				const room = rooms.get(code);
				if (!room || !room.host || room.guest) {
					send(ws, { type: 'error', message: 'Oda bulunamadı ya da zaten dolu.' });
					return;
				}
				room.guest = ws;
				ws.roomCode = code;
				pair(room.host, ws, true);
				break;
			}
			case 'random_match': {
				// Kuyrukta artık geçerli (hâlâ açık) biri var mı diye ara.
				while (randomQueue.length > 0) {
					const otherWs = randomQueue.shift();
					if (otherWs.readyState === WebSocket.OPEN) {
						pair(otherWs, ws, true); // kuyrukta önce bekleyen "host" sayılır
						return;
					}
				}
				randomQueue.push(ws);
				send(ws, { type: 'waiting' });
				break;
			}
			case 'cancel': {
				cleanupConnection(ws);
				ws.roomCode = null;
				ws.peer = null;
				break;
			}
			case 'relay': {
				if (ws.peer) {
					send(ws.peer, { type: 'relay', payload: msg.payload });
				}
				break;
			}
			default:
				break;
		}
	});

	ws.on('close', () => {
		cleanupConnection(ws);
	});

	ws.on('error', () => {
		cleanupConnection(ws);
	});
});

console.log(`Kingsgrid relay sunucusu ${PORT} portunda çalışıyor.`);
