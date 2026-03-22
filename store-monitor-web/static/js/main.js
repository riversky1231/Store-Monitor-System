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

  const networkToast = document.getElementById("networkIssueToast");
  const networkToastTitle = document.getElementById("networkIssueToastTitle");
  const networkToastMessage = document.getElementById("networkIssueToastMessage");
  const networkToastDetail = document.getElementById("networkIssueToastDetail");
  const networkToastMeta = document.getElementById("networkIssueToastMeta");
  const networkToastClose = document.getElementById("networkIssueToastClose");
  const networkToastAck = document.getElementById("networkIssueToastAck");
  const LAST_ISSUE_AT_KEY = "store-monitor.network-alert.issue.last-at";
  const LAST_ISSUE_EVENT_KEY = "store-monitor.network-alert.issue.last-event";
  const LAST_RECOVERY_AT_KEY = "store-monitor.network-alert.recovery.last-at";
  const LAST_RECOVERY_EVENT_KEY = "store-monitor.network-alert.recovery.last-event";
  const LAST_BROWSER_NOTICE_KEY = "store-monitor.network-alert.browser.last";
  let currentNetworkAlertEventId = 0;
  let currentNetworkAlertState = "idle";

  function hideNetworkToast() {
    if (!networkToast) {
      return;
    }
    networkToast.classList.remove("is-issue", "is-recovered");
    networkToast.classList.remove("show");
    window.setTimeout(() => {
      if (!networkToast.classList.contains("show")) {
        networkToast.hidden = true;
      }
    }, 220);
  }

  function alertStorageKeys(type) {
    if (type === "recovered") {
      return { at: LAST_RECOVERY_AT_KEY, event: LAST_RECOVERY_EVENT_KEY };
    }
    return { at: LAST_ISSUE_AT_KEY, event: LAST_ISSUE_EVENT_KEY };
  }

  function rememberNetworkAlert(type, eventId) {
    const keys = alertStorageKeys(type);
    try {
      window.localStorage.setItem(keys.at, String(Date.now()));
      if (eventId) {
        window.localStorage.setItem(keys.event, String(eventId));
      }
    } catch (_err) {}
  }

  function readRememberedNetworkAlert(type) {
    const keys = alertStorageKeys(type);
    let lastShownAt = 0;
    let lastEventId = 0;
    try {
      lastShownAt = Number(window.localStorage.getItem(keys.at) || "0");
      lastEventId = Number(window.localStorage.getItem(keys.event) || "0");
    } catch (_err) {}
    return { lastShownAt, lastEventId };
  }

  function shouldShowNetworkAlert(data) {
    if (!data || !data.state || data.state === "idle") {
      return false;
    }
    const cooldownMs = Math.max(60000, Number(data.cooldown_seconds || 900) * 1000);
    const remembered = readRememberedNetworkAlert(data.state);
    const lastShownAt = remembered.lastShownAt;
    const lastEventId = remembered.lastEventId;
    if (Number(data.event_id || 0) > lastEventId) {
      return true;
    }
    if (data.state === "recovered") {
      return false;
    }
    return Date.now() - lastShownAt >= cooldownMs;
  }

  function renderNetworkAlertMeta(data) {
    const parts = [];
    if (data.state === "recovered") {
      parts.push("系统已恢复自动重试");
    } else if (data.pending_count) {
      parts.push("待重试任务 " + data.pending_count + " 个");
    }
    if (Array.isArray(data.pending_preview) && data.pending_preview.length) {
      parts.push("涉及: " + data.pending_preview.join("、"));
    }
    return parts.join(" · ");
  }

  function maybeSendBrowserNotification(data) {
    if (!data || !data.state || !("Notification" in window)) {
      return;
    }
    if (window.Notification.permission !== "granted") {
      return;
    }
    const browserEventKey = data.state + ":" + String(data.event_id || 0);
    try {
      if (window.localStorage.getItem(LAST_BROWSER_NOTICE_KEY) === browserEventKey) {
        return;
      }
    } catch (_err) {}
    try {
      const body = [data.message, data.detail].filter(Boolean).join("\n");
      new window.Notification(data.title || "系统提醒", { body });
      window.localStorage.setItem(LAST_BROWSER_NOTICE_KEY, browserEventKey);
    } catch (_err) {}
  }

  function showNetworkToast(data) {
    if (!networkToast) {
      return;
    }
    currentNetworkAlertEventId = Number(data.event_id || 0);
    currentNetworkAlertState = data.state || "issue";
    networkToast.classList.remove("is-issue", "is-recovered");
    networkToast.classList.add(currentNetworkAlertState === "recovered" ? "is-recovered" : "is-issue");
    networkToastTitle.textContent = data.title || "抓取网络异常提醒";
    networkToastMessage.textContent = data.message || "检测到抓取网络或访问异常。";
    networkToastDetail.textContent = data.detail || "建议检查当前网络、代理或出口节点后等待系统自动重试。";
    networkToastMeta.textContent = renderNetworkAlertMeta(data);
    networkToast.hidden = false;
    window.requestAnimationFrame(() => {
      networkToast.classList.add("show");
    });
    rememberNetworkAlert(currentNetworkAlertState, currentNetworkAlertEventId);
    maybeSendBrowserNotification(data);
  }

  function pollNetworkAlert() {
    if (!networkToast) {
      return;
    }
    window.fetch("/api/network-alert-status", { cache: "no-store" })
      .then((response) => response.json())
      .then((data) => {
        if (!data.state || data.state === "idle") {
          currentNetworkAlertEventId = Number(data.event_id || 0);
          currentNetworkAlertState = "idle";
          hideNetworkToast();
          return;
        }
        if (shouldShowNetworkAlert(data)) {
          showNetworkToast(data);
          return;
        }
        if (!networkToast.hidden) {
          currentNetworkAlertEventId = Number(data.event_id || 0);
          currentNetworkAlertState = data.state || currentNetworkAlertState;
          networkToast.classList.remove("is-issue", "is-recovered");
          networkToast.classList.add(currentNetworkAlertState === "recovered" ? "is-recovered" : "is-issue");
          networkToastMessage.textContent = data.message || networkToastMessage.textContent;
          networkToastDetail.textContent = data.detail || networkToastDetail.textContent;
          networkToastMeta.textContent = renderNetworkAlertMeta(data);
        }
      })
      .catch(() => {});
  }

  if (networkToastClose) {
    networkToastClose.addEventListener("click", () => {
      rememberNetworkAlert(currentNetworkAlertState, currentNetworkAlertEventId);
      hideNetworkToast();
    });
  }

  if (networkToastAck) {
    networkToastAck.addEventListener("click", () => {
      rememberNetworkAlert(currentNetworkAlertState, currentNetworkAlertEventId);
      hideNetworkToast();
    });
  }

  if (networkToast) {
    pollNetworkAlert();
    window.setInterval(pollNetworkAlert, 30000);
  }
})();
