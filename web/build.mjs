// build.mjs - copies web/src/** and web/public/** into web/dist.
// Plain node only; no minification, no bundling.
import { existsSync } from 'node:fs';
import { cpSync, mkdirSync, rmSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(fileURLToPath(import.meta.url));
const src = join(root, 'src');
const pub = join(root, 'public');
const dist = join(root, 'dist');

rmSync(dist, { recursive: true, force: true });
mkdirSync(dist, { recursive: true });

if (existsSync(src)) cpSync(src, dist, { recursive: true });
if (existsSync(pub)) cpSync(pub, dist, { recursive: true });

console.log('krply-web: built dist/ from src/ (and public/)');
