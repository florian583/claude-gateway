# Stage-only helper: install with matching gateway header stripping support.
# Safe to source repeatedly and in inherited worker environments. No network.
gateway_usage_tag() {
  local usage_key="$1" usage_value="$2"
  [ -n "$usage_value" ] && [ "${#usage_value}" -le 96 ] || return 0
  case "$usage_value" in *[!a-zA-Z0-9_.:-]*) return 0 ;; esac
  ANTHROPIC_CUSTOM_HEADERS="${ANTHROPIC_CUSTOM_HEADERS:+${ANTHROPIC_CUSTOM_HEADERS}
}${usage_key}: ${usage_value}"
}

# Remove only owned headers, even when TAGGED was inherited. Keep all others.
gateway_usage_headers=""
while IFS= read -r gateway_usage_line || [ -n "$gateway_usage_line" ]; do
  case "$gateway_usage_line" in
    [Xx]-[Gg][Aa][Tt][Ee][Ww][Aa][Yy]-*) ;;
    '') ;;
    *) gateway_usage_headers="${gateway_usage_headers:+${gateway_usage_headers}
}${gateway_usage_line}" ;;
  esac
done <<GATEWAY_USAGE_HEADERS
${ANTHROPIC_CUSTOM_HEADERS:-}
GATEWAY_USAGE_HEADERS
ANTHROPIC_CUSTOM_HEADERS="$gateway_usage_headers"

# Parent identity stays distinct; an explicit child session never inherits it
# as its own identity. Native clients still supply their own session IDs.
if [ -n "${GATEWAY_SESSION:-}" ] && [ -n "${GATEWAY_WORKER:-}" ] && [ "${GATEWAY_USAGE_TAGGED_WORKER:-}" != "$GATEWAY_WORKER" ]; then
  GATEWAY_PARENT_SESSION="${GATEWAY_PARENT_SESSION:-$GATEWAY_SESSION}"
fi
if [ -n "${WELL_PR_BOT_LANE:-}" ]; then
  GATEWAY_WORKLOAD=author-bot
  GATEWAY_LANE="$WELL_PR_BOT_LANE"
  GATEWAY_RUN="${WELL_PR_BOT_RUN_ID:-}"
elif [ -z "${GATEWAY_WORKLOAD:-}" ] && [ -t 0 ]; then
  GATEWAY_WORKLOAD=interactive
fi
gateway_usage_tag X-Gateway-Workload "${GATEWAY_WORKLOAD:-}"
gateway_usage_tag X-Gateway-Lane "${GATEWAY_LANE:-}"
gateway_usage_tag X-Gateway-Run "${GATEWAY_RUN:-}"
gateway_usage_tag X-Gateway-Origin "${GATEWAY_ORIGIN:-}"
gateway_usage_tag X-Gateway-Host "${GATEWAY_HOST:-}"
gateway_usage_tag X-Gateway-Parent-Session "${GATEWAY_PARENT_SESSION:-}"
gateway_usage_tag X-Gateway-Worker "${GATEWAY_WORKER:-}"
gateway_usage_tag X-Gateway-PR "${GATEWAY_PR:-}"
gateway_usage_tag X-Gateway-Head "${GATEWAY_HEAD:-}"
gateway_usage_tag X-Gateway-Stage "${GATEWAY_STAGE:-}"
export ANTHROPIC_CUSTOM_HEADERS GATEWAY_USAGE_TAGGED=1
export GATEWAY_PARENT_SESSION GATEWAY_USAGE_TAGGED_WORKER="${GATEWAY_WORKER:-}"
unset gateway_usage_headers gateway_usage_line
unset -f gateway_usage_tag
