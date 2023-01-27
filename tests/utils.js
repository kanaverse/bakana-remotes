import * as fs from "fs";
import * as bakana from "bakana";
import * as utils from "../src/utils.js";
import * as valkana from "valkana";
import "isomorphic-fetch";

export async function initializeAll() {
    await bakana.initialize({ localFile: true });
    await valkana.initialize({ localFile: true });
}

export async function downloader(url) {
    if (!fs.existsSync("files")) {
        fs.mkdirSync("files");
    }
    let path = "files/" + encodeURIComponent(url);

    if (!fs.existsSync(path)) {
        let res = await fetch(url);
        if (!res.ok) {
            throw new Error("failed to fetch resource at '" + url + "' (" + String(res.status) + ")");
        }

        let contents = new Uint8Array(await res.arrayBuffer());
        fs.writeFileSync(path, contents);
        return contents;
    }

    // Node.js buffers are Uint8Arrays, so we can just return it without casting,
    // though we need to make a copy to avoid overwriting.
    return fs.readFileSync(path).slice();
}

bakana.setCellLabellingDownload(downloader);

export function baseParams() {
    let output = bakana.analysisDefaults();

    // Cut down on the work.
    output.rna_pca.num_pcs = 10;

    // Avoid getting held up by pointless iterations.
    output.tsne.iterations = 10;
    output.umap.num_epochs = 10;

    // Actually do something.
    output.cell_labelling = {
        mouse_references: [ "ImmGen" ],
        human_references: [ "BlueprintEncode" ]
    };
    return output;
}

export function validateState(path, embedded = true) {
    valkana.validateState(path, embedded, bakana.kanaFormatVersion);
}

export function mockOffsets(buffers) {
    let offsets = {};
    let sofar = 0;
    for (const b of buffers) {
        offsets[sofar] = b;
        sofar += b.length;
    }
    return offsets;
}
