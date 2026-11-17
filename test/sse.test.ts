import assert from 'node:assert/strict';
import { test } from 'node:test';

import { SseParser, VoiceAgentStreamError } from '../src/index.js';

test('parses messages split across chunks, with CRLF, comments and multi-line data', () => {
    const parser = new SseParser();

    assert.equal(parser.feed(': hello\r\nid: a-1\r\nevent: agent.conn').length, 0);
    assert.deepEqual(parser.feed('ected\r\ndata: {"x":1}\r\n\r\nretry: 250\ndata: line1\ndata:line2\n\n'), [
        { data: '{"x":1}', event: 'agent.connected', id: 'a-1' },
        { data: 'line1\nline2', id: 'a-1', retry: 250 }
    ]);
});

test('a CR that ends a chunk and a LF that opens the next are one line end', () => {
    const parser = new SseParser();

    assert.equal(parser.feed('data: x\r').length, 0);
    assert.deepEqual(parser.feed('\n\n'), [ { data: 'x' } ]);
    assert.equal(parser.feed('data: a\r').length, 0);
    assert.deepEqual(parser.feed('data: b\r\r'), [ { data: 'a\nb' } ], 'a CR followed by other text is a line end by itself');
    assert.deepEqual(parser.feed('\ndata: c\n\n'), [ { data: 'c' } ]);
    const split = new SseParser();

    assert.equal(split.feed('data: a\r').length, 0);
    assert.deepEqual(split.feed('\ndata: b\n\n'), [ { data: 'a\nb' } ], 'a LF counted as a second line end would dispatch a alone');
});

test('ignores a BOM, an id with NUL, a non-numeric retry and unknown fields', () => {
    const parser = new SseParser();

    assert.deepEqual(parser.feed('﻿id: bad\0id\nretry: soon\nfoo: bar\ndata: 1\n\n'), [ { data: '1' } ]);
});

test('refuses a line or a message longer than its limit', () => {
    assert.throws(() => new SseParser(64).feed('x'.repeat(100)), VoiceAgentStreamError);
    const parser = new SseParser(64);

    assert.deepEqual(parser.feed('data: ' + 'a'.repeat(40) + '\n'), []);
    assert.throws(() => parser.feed('data: ' + 'b'.repeat(40) + '\n'), VoiceAgentStreamError);
    assert.deepEqual(new SseParser(64).feed('data: ' + 'c'.repeat(40) + '\n\ndata: ' + 'd'.repeat(40) + '\n\n').map(m => m.data.length), [ 40, 40 ]);
});

test('counts data lines without a value, and a line that arrives in pieces, against the limit', () => {
    assert.throws(() => new SseParser(64).feed('data:\n'.repeat(100)), VoiceAgentStreamError);
    const parser = new SseParser(64);

    for (let i = 0; i < 6; i++) {
        assert.deepEqual(parser.feed('0123456789'), []);
    }
    assert.throws(() => parser.feed('0123456789'), VoiceAgentStreamError);
    const pieces = new SseParser(64);

    assert.deepEqual([ 'da', 'ta: ', 'on', 'e\n', '\n' ].flatMap(piece => pieces.feed(piece)), [ { data: 'one' } ]);
    assert.equal(new SseParser(10).feed('data: 0123456789\n\n')[0]?.data.length, 10, 'a message of exactly the limit passes');
    assert.equal(new SseParser(10).feed('data: 01234\ndata: 0123\n\n')[0]?.data.length, 10);
    assert.throws(() => new SseParser(10).feed('data: 01234\ndata: 01234\n\n'), VoiceAgentStreamError);
});

test('a blank line without data dispatches nothing and resets the event name', () => {
    const parser = new SseParser();

    assert.deepEqual(parser.feed('event: snapshot\n\n'), []);
    assert.deepEqual(parser.feed('data: {}\n\n'), [ { data: '{}' } ]);
});
