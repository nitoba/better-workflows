let audit = (event) => process.send?.(event)

export function setAudit(callback) {
  audit = callback
}

export function recordAudit(event) {
  audit(event)
}
