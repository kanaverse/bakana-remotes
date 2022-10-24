export var downloadFun = async url => {
    let resp = await fetch(url);
    if (!resp.ok) {
        throw new Error("failed to fetch content at " + url + "(" + resp.status + ")");
    }
    return await resp.arrayBuffer();
};

/**
 * Specify a function to download references for the cell labelling step.
 *
 * @param {function} fun - Function that accepts a single string containing a URL,
 * and returns an ArrayBuffer of that URL's contents.
 *
 * @return `fun` is set as the global downloader for this step.
 * The _previous_ value of the downloader is returned.
 */
export function setDownloadFun(fun) {
    let previous = downloadFun;
    downloadFun = fun;
    return previous;
}


