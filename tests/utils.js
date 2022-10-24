import * as fs from "fs";
import * as bakana from "bakana";
import * as utils from "../src/utils.js";
import "isomorphic-fetch";

export async function initializeAll() {
    await bakana.initialize({ localFile: true });
}

utils.setDownloadFun(async url => {
    let path = "files/" + encodeURIComponent(url);

    if (!fs.existsSync(path)) {
        let res = await fetch(url);
        if (!res.ok) {
            throw new Error("failed to fetch resource at '" + url + "' (" + String(res.status) + ")");
        }

        let contents = await res.arrayBuffer();
        fs.writeFileSync(path, new Uint8Array(contents));
        return contents;
    }

    let obj = fs.readFileSync(path);
    return obj.buffer.slice(obj.byteOffset, obj.byteOffset + obj.byteLength);
});

export function baseParams() {
    let output = bakana.analysisDefaults();

    // Cut down on the work.
    output.pca.num_pcs = 10;

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
