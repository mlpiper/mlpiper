const link = document.getElementById('mlops-api');
if (link !== null) {
  // By using '_self' as the target, the browser thinks we are replacing the current window
  // contents, and not opening a popup, which gets blocked.  The previous
  // scheme had the click as a primary user action, so it WOULD open the new
  // window.  This way we get the mlops docs in the same window, replacing the
  // original documentation, and avoiding window proliferation.
  window.open(window.location.origin + '/docs/mlops', '_self');
}