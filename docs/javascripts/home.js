/* Landing page behaviour for dsc-databricks.
 *
 * A vanilla-JS reimplementation of the parallax component that shipped with
 * the Material for MkDocs website (components/parallax/index.ts, MIT). The
 * parallax motion itself is pure CSS — see docs/stylesheets/home.css. This
 * script only does three things: reveal sections as they scroll into view,
 * give the header a background once the hero is behind us, and work around
 * two Firefox rendering bugs.
 */

(function () {
  "use strict";

  var parallax = document.querySelector("[data-mdx-component=parallax]");
  if (!parallax) {
    return;
  }

  /* ---------------------------------------------------------------------- */
  /* Scroll reveal                                                          */
  /* ---------------------------------------------------------------------- */

  /* Sections start with the `hidden` attribute, which the stylesheet turns
     into "transparent and offset" rather than "display: none" — but only
     under `.js`, so without scripting everything stays readable. Removing the
     attribute lets the transition run. */
  var targets = parallax.querySelectorAll("[hidden]");

  if ("IntersectionObserver" in window) {
    var observer = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            entry.target.hidden = false;
            observer.unobserve(entry.target);
          }
        });
      },
      { root: parallax }
    );

    Array.prototype.forEach.call(targets, function (target) {
      observer.observe(target);
    });
  } else {
    Array.prototype.forEach.call(targets, function (target) {
      target.hidden = false;
    });
  }

  /* ---------------------------------------------------------------------- */
  /* Header shadow                                                          */
  /* ---------------------------------------------------------------------- */

  var header = document.querySelector(".md-header");
  var second = parallax.children[1];

  function updateHeader() {
    if (header && second) {
      header.classList.toggle(
        "md-header--shadow",
        parallax.scrollTop > second.offsetTop
      );
    }
  }

  /* ---------------------------------------------------------------------- */
  /* Firefox workarounds                                                    */
  /* ---------------------------------------------------------------------- */

  var isGecko = navigator.userAgent.indexOf("Gecko/") !== -1;
  var hero = document.querySelector("[data-mdx-component=hero]");

  function updateGeckoHacks() {
    if (!isGecko) {
      return;
    }

    /* Firefox mis-renders the sticky hero the moment the container scrolls,
       so it is taken out entirely once scrolling starts. */
    if (hero) {
      hero.hidden = parallax.scrollTop > 1;
    }

    /* And `contain: strict` on the hero group breaks painting further down
       the page, so containment is reset until the reader comes back up. */
    document.body.classList.toggle("ff-hack", parallax.scrollTop <= 3000);
  }

  /* ---------------------------------------------------------------------- */

  var ticking = false;

  parallax.addEventListener(
    "scroll",
    function () {
      if (ticking) {
        return;
      }
      ticking = true;
      window.requestAnimationFrame(function () {
        updateHeader();
        updateGeckoHacks();
        ticking = false;
      });
    },
    { passive: true }
  );

  updateHeader();
  updateGeckoHacks();
})();
