(function () {
  const body = document.body;
  const menuToggle = document.getElementById("menuToggle");
  const sidebarClose = document.getElementById("sidebarClose");
  const navScrim = document.getElementById("navScrim");

  function openNav() {
    body.classList.add("nav-open");
  }

  function closeNav() {
    body.classList.remove("nav-open");
  }

  if (menuToggle) {
    menuToggle.addEventListener("click", () => {
      if (body.classList.contains("nav-open")) {
        closeNav();
      } else {
        openNav();
      }
    });
  }

  if (sidebarClose) {
    sidebarClose.addEventListener("click", closeNav);
  }

  if (navScrim) {
    navScrim.addEventListener("click", closeNav);
  }

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      closeNav();
      document.querySelectorAll(".overlay.show").forEach((el) => el.classList.remove("show"));
    }
  });

  const revealTargets = document.querySelectorAll(".stat, .card, .settings-section, .setup-box");
  revealTargets.forEach((el, index) => {
    const delay = Math.min(index * 70, 420);
    window.setTimeout(() => {
      el.classList.add("is-visible");
    }, delay);
  });
})();
