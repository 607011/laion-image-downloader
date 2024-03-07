(function (window) {
    "use strict";

    const el = {};

    function download(filename, data) {
        const element = document.createElement("a");
        element.setAttribute("href", `data:text/csv;charset=utf-8,${encodeURIComponent(data)}`);
        element.setAttribute("download", filename);
        element.style.display = "none";
        document.body.appendChild(element);
        element.click();
        document.body.removeChild(element);
    }

    function updateSelected() {
        el.selectCount.innerText = document.querySelectorAll("#table>a.selected").length;
    }

    function onTileClicked(e) {
        e.target.classList.toggle("selected");
        updateSelected();
        document.querySelector("#table").focus();
        return e.preventDefault();
    }

    function onKeyDown(e) {
        if ((e.metaKey || e.ctrlKey) && e.key === "a") {
            for (const a of document.querySelectorAll("#table>a")) {
                a.classList.add("selected");
            }
            updateSelected();
            return e.preventDefault();
        }
        else if ((e.metaKey || e.ctrlKey) && e.key === "i") {
            for (const a of document.querySelectorAll("#table>a")) {
                a.classList.toggle("selected");
            }
            updateSelected();
            return e.preventDefault();
        }
        else if ((e.metaKey || e.ctrlKey) && e.key === "d") {
            for (const a of document.querySelectorAll("#table>a")) {
                a.classList.remove("selected");
            }
            updateSelected();
            return e.preventDefault();
        }
        else if ((e.metaKey || e.ctrlKey) && e.key === "s") {
            const selected =
                [...document.querySelectorAll("#table>a.selected")]
                    .map(el => {
                        const urlMatch = el.style.backgroundImage.match(/url\("(.*)"\)/);
                        return `${el.getAttribute("data-hash")};${urlMatch[1]};${el.href}`;
                    });
            if (selected.length > 0) {
                download("selected-hashes.csv", selected.join("\n"));                
            }
            return e.preventDefault();
        }
    }

    function main() {
        el.selectCount = document.querySelector("#select-count");
        document.querySelectorAll("#table>a")
            .forEach(a => a.addEventListener("click", onTileClicked));
        document.addEventListener("keydown", onKeyDown);
        document.querySelector("#table").focus();
    }

    window.addEventListener("load", main);
})(window)