import * as fs from "fs";
import * as bakana from "bakana";
import * as utils from "../src/utils.js";
import "isomorphic-fetch";

export async function initializeAll() {
    await bakana.initialize({ localFile: true });
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

bakana.FeatureSetEnrichmentState.setDownload(downloader);
bakana.CellLabellingState.setDownload(downloader);
bakana.RnaQualityControlState.setDownload(downloader);

export function baseParams() {
    let output = bakana.analysisDefaults();

    // Cut down on the work.
    output.rna_pca.num_pcs = 10;

    // Avoid getting held up by pointless iterations.
    output.tsne.iterations = 10;
    output.umap.num_epochs = 10;

    return output;
}
