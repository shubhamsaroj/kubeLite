// Runs before first paint so the page never flashes the wrong theme.
(function () {
  var pref = localStorage.getItem("kubelite.theme") || "system";
  var dark = pref === "dark" || (pref === "system" && matchMedia("(prefers-color-scheme: dark)").matches);
  document.documentElement.dataset.theme = dark ? "dark" : "light";
})();
