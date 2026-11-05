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

// KOPMA TOLERANSI: eşleşmiş iki oyuncudan biri (ağ kesintisi, Wi-Fi atlaması vb.)
// düşerse, eşleşme HEMEN yıkılmaz; GRACE_MS boyunca bekletilir. Düşen oyuncu
// eşleşmede aldığı "token" ile geri bağlanırsa ("resume") maç kaldığı yerden
// devam eder. Süre dolarsa kalan oyuncuya "peer_disconnected" gider (o kazanır).
// Oyuncu maçtan KASTEN ayrılırsa ("cancel") bekleme yoktur, hemen bildirilir.
const GRACE_MS = parseInt(process.env.GRACE_MS || '20000', 10);
const HEARTBEAT_MS = 10 * 1000;   // sessizce ölen bağlantıları (half-open) yakalar

// rooms: kod -> { host: ws, guest: ws|null }
const rooms = new Map();
// pairs: pairId -> { sides:[ws,ws], tokens:[t,t], roles:['host','guest'], lost:[bool,bool], timer }
const pairs = new Map();
const tokenToPair = new Map();
// randomQueue: rastgele eşleşme bekleyen bağlantılar (FIFO).
let randomQueue = [];

const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // karışabilecek 0/O, 1/I çıkarıldı
const CODE_LENGTH = 5;
const ROOM_TIMEOUT_MS = 10 * 60 * 1000; // misafir gelmezse 10 dakika sonra oda otomatik silinir

function generateToken() {
	let t;
	do {
		t = '';
		for (let i = 0; i < 20; i++) t += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
	} while (tokenToPair.has(t));
	return t;
}

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

function endPair(pair, notifyConnected) {
	if (pair.timer) clearTimeout(pair.timer);
	pair.timer = null;
	for (let i = 0; i < 2; i++) {
		const w = pair.sides[i];
		if (notifyConnected && w && !pair.lost[i]) {
			send(w, { type: 'peer_disconnected' });
		}
		if (w && w.pairObj === pair) {
			w.pairObj = null;
			w.peer = null;
		}
		tokenToPair.delete(pair.tokens[i]);
	}
	pairs.delete(pair.id);
}

function markLost(ws) {
	const pair = ws.pairObj;
	if (!pair) return;
	const i = pair.sides.indexOf(ws);
	if (i < 0 || pair.lost[i]) return;
	pair.lost[i] = true;
	ws.peer = null;
	const other = pair.sides[1 - i];
	if (other && !pair.lost[1 - i]) {
		other.peer = null;
		send(other, { type: 'peer_lost', grace: Math.round(GRACE_MS / 1000) });
	}
	if (!pair.timer) {
		pair.timer = setTimeout(() => endPair(pair, true), GRACE_MS);
	}
}

// voluntary=true: oyuncu kasıtlı ayrıldı (bekleme yok). false: soket düştü (bekleme var).
function cleanupConnection(ws, voluntary) {
	randomQueue = randomQueue.filter((w) => w !== ws);
	if (ws.roomCode && rooms.has(ws.roomCode)) {
		const room = rooms.get(ws.roomCode);
		if (room.timeoutHandle) clearTimeout(room.timeoutHandle);
		rooms.delete(ws.roomCode);
	}
	const pair = ws.pairObj;
	if (pair) {
		if (voluntary || GRACE_MS <= 0) {
			const i = pair.sides.indexOf(ws);
			if (i >= 0) pair.lost[i] = true;   // ayrılan taraf bildirim ALMAZ
			endPair(pair, true);
		} else {
			markLost(ws);
		}
		return;
	}
	if (ws.peer) {
		send(ws.peer, { type: 'peer_disconnected' });
		ws.peer.peer = null;
	}
}

function pair(peerA, peerB, roleAIsHost) {
	if (peerA.roomCode && rooms.has(peerA.roomCode)) {
		const room = rooms.get(peerA.roomCode);
		if (room.timeoutHandle) clearTimeout(room.timeoutHandle);
	}
	const p = {
		id: generateToken(),
		sides: [peerA, peerB],
		tokens: [generateToken(), null],
		roles: [roleAIsHost ? 'host' : 'guest', roleAIsHost ? 'guest' : 'host'],
		lost: [false, false],
		timer: null,
	};
	p.tokens[1] = generateToken();
	pairs.set(p.id, p);
	tokenToPair.set(p.tokens[0], p);
	tokenToPair.set(p.tokens[1], p);
	peerA.pairObj = p;
	peerB.pairObj = p;
	peerA.peer = peerB;
	peerB.peer = peerA;
	const grace = Math.round(GRACE_MS / 1000);
	send(peerA, { type: 'paired', role: p.roles[0], token: p.tokens[0], grace });
	send(peerB, { type: 'paired', role: p.roles[1], token: p.tokens[1], grace });
}

function resume(ws, token) {
	const pair = tokenToPair.get(String(token || ''));
	if (!pair) {
		send(ws, { type: 'resume_failed' });
		return;
	}
	const i = pair.tokens.indexOf(token);
	// Eski soket hâlâ "açık" görünüyorsa (yarım kalmış bağlantı) önce onu düşür.
	const old = pair.sides[i];
	if (old && old !== ws) {
		if (!pair.lost[i]) markLost(old);
		old.pairObj = null;
		old.peer = null;
		try { old.terminate(); } catch (e) { /* yok say */ }
	}
	pair.sides[i] = ws;
	pair.lost[i] = false;
	ws.pairObj = pair;
	const other = pair.sides[1 - i];
	const otherOnline = !!other && !pair.lost[1 - i];
	if (otherOnline) {
		ws.peer = other;
		other.peer = ws;
		if (pair.timer) { clearTimeout(pair.timer); pair.timer = null; }
	}
	send(ws, { type: 'resumed', role: pair.roles[i], peer_online: otherOnline });
	if (otherOnline) send(other, { type: 'peer_resumed' });
}

wss.on('connection', (ws) => {
	ws.peer = null;
	ws.roomCode = null;
	ws.pairObj = null;
	ws.isAlive = true;
	ws.on('pong', () => { ws.isAlive = true; });

	ws.on('message', (raw) => {
		let msg;
		try {
			msg = JSON.parse(raw.toString());
		} catch (e) {
			return; // bozuk/JSON-olmayan mesajı sessizce yok say
		}

		switch (msg.type) {
			case 'create_room': {
				if (ws.pairObj || ws.peer) break;
				if (ws.roomCode && rooms.has(ws.roomCode)) {
					const prev = rooms.get(ws.roomCode);
					if (prev.host === ws && !prev.guest) {
						if (prev.timeoutHandle) clearTimeout(prev.timeoutHandle);
						rooms.delete(ws.roomCode);
					}
				}
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
				if (!room || !room.host || room.guest || room.host === ws) {
					send(ws, { type: 'error', message: 'Oda bulunamadı ya da zaten dolu.' });
					return;
				}
				room.guest = ws;
				ws.roomCode = code;
				pair(room.host, ws, true);
				break;
			}
			case 'random_match': {
				// Zaten eşleşmiş ya da bir ortağı olan bağlantı tekrar kuyruğa girmez.
				if (ws.pairObj || ws.peer) break;
				// Düğmeye art arda basılırsa aynı bağlantı kuyruğa ikinci kez girmesin
				// (aksi halde kendiyle eşleşirdi): sadece "bekleniyor" bilgisini tekrarla.
				if (randomQueue.includes(ws)) {
					send(ws, { type: 'waiting' });
					break;
				}
				// Kuyrukta artık geçerli (hâlâ açık) biri var mı diye ara.
				while (randomQueue.length > 0) {
					const otherWs = randomQueue.shift();
					if (otherWs !== ws && otherWs.readyState === WebSocket.OPEN) {
						pair(otherWs, ws, true); // kuyrukta önce bekleyen "host" sayılır
						return;
					}
				}
				randomQueue.push(ws);
				send(ws, { type: 'waiting' });
				break;
			}
			case 'cancel': {
				cleanupConnection(ws, true);
				ws.roomCode = null;
				ws.peer = null;
				break;
			}
			case 'resume': {
				resume(ws, msg.token);
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
		cleanupConnection(ws, false);
	});

	ws.on('error', () => {
		cleanupConnection(ws, false);
	});
});

// Kalp atışı: yanıt vermeyen soketi kapat → "close" → bekleme süresi başlar.
setInterval(() => {
	wss.clients.forEach((ws) => {
		if (ws.isAlive === false) {
			ws.terminate();
			return;
		}
		ws.isAlive = false;
		try { ws.ping(); } catch (e) { /* yok say */ }
	});
}, HEARTBEAT_MS);

console.log(`Kingsgrid relay sunucusu ${PORT} portunda çalışıyor.`);
