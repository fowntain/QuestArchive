import { config } from './config.js';
import { s3Client } from './s3.js';
import { hashId, readJsonFromR2, writeJsonToR2, deleteObjectFromR2 } from './s3-helpers.js';

const md5MapState = {
    lastSyncAt: 0,
    syncing: null,
    map: null
};

function shallowEqualObject(a, b) {
    if (a === b) return true;
    if (!a || !b) return false;
    const ak = Object.keys(a);
    const bk = Object.keys(b);
    if (ak.length !== bk.length) return false;
    return ak.every(k => a[k] === b[k]);
}

async function doSyncMd5Map() {
    try {
        const { ListObjectsV2Command } = await import('@aws-sdk/client-s3');
        const zipKeys = [];
        let token = undefined;

        while (true) {
            const objectsRes = await s3Client.send(new ListObjectsV2Command({
                Bucket: config.R2.BUCKET_NAME,
                Delimiter: '/',
                ContinuationToken: token
            }));

            for (const obj of objectsRes.Contents || []) {
                const key = obj?.Key;
                if (!key) continue;
                if (key.includes('/')) continue;
                if (!key.toLowerCase().endsWith('.zip')) continue;
                zipKeys.push(key);
            }

            if (!objectsRes.IsTruncated) break;
            token = objectsRes.NextContinuationToken;
            if (!token) break;
        }

        const newMap = {};
        for (const key of zipKeys) {
            const base = key.replace(/\.zip$/i, '');
            const hash = hashId(base);
            newMap[hash] = key;
        }

        const legacyKey = config.R2.LEGACY_MD5_MAP_KEY;
        const primary = await readJsonFromR2(config.R2.MD5_MAP_KEY, null);
        const legacy = legacyKey && legacyKey !== config.R2.MD5_MAP_KEY
            ? await readJsonFromR2(legacyKey, null)
            : null;

        if (primary === null && legacy && typeof legacy === 'object') {
            await writeJsonToR2(config.R2.MD5_MAP_KEY, legacy);
            await deleteObjectFromR2(legacyKey);
        }

        const remote = await readJsonFromR2(config.R2.MD5_MAP_KEY, {});
        if (!shallowEqualObject(newMap, remote)) {
            await writeJsonToR2(config.R2.MD5_MAP_KEY, newMap);
        }

        md5MapState.map = newMap;
        md5MapState.lastSyncAt = Date.now();
    } catch (err) {
        console.error('Error syncing MD5 map:', err);
        if (!md5MapState.map) {
            md5MapState.map = await readJsonFromR2(config.R2.MD5_MAP_KEY, {});
        }
    } finally {
        md5MapState.syncing = null;
    }
}

export async function ensureMd5MapFresh({ force = false } = {}) {
    const now = Date.now();
    const age = now - md5MapState.lastSyncAt;
    const needsSync = force || age > 30 * 60 * 1000;

    if (needsSync && !md5MapState.syncing) {
        md5MapState.syncing = doSyncMd5Map();
    }

    if (force) await md5MapState.syncing;
    if (md5MapState.map) return md5MapState.map;

    const legacyKey = config.R2.LEGACY_MD5_MAP_KEY;
    const primary = await readJsonFromR2(config.R2.MD5_MAP_KEY, null);
    if (primary !== null) return primary || {};
    if (legacyKey && legacyKey !== config.R2.MD5_MAP_KEY) {
        const legacy = await readJsonFromR2(legacyKey, null);
        if (legacy !== null) {
            await writeJsonToR2(config.R2.MD5_MAP_KEY, legacy);
            await deleteObjectFromR2(legacyKey);
            return legacy || {};
        }
    }
    return {};
}

export async function findKeyByHash(hash) {
    const map = await ensureMd5MapFresh({ force: false });
    const originalName = map[hash];
    if (!originalName) return null;
    return originalName.toLowerCase().endsWith('.zip') ? originalName : `${originalName}.zip`;
}
