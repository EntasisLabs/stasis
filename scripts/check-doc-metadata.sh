#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

files=(
  "docs/architecture/overview.md"
  "docs/v1-runtime-draft.md"
  "docs/design/job-runtime-design.md"
  "docs/architecture/surrealdb-schema.md"
  "docs/design/stasis-framework-rfc.md"
  "docs/adr/README.md"
  "docs-book/src/architecture-overview.md"
  "docs-book/src/runtime-builder.md"
  "docs-book/src/runtime-job-design.md"
  "docs-book/src/surrealdb-schema.md"
  "docs-book/src/adr.md"
)

fail_count=0

trim() {
  local s="$1"
  s="${s#${s%%[![:space:]]*}}"
  s="${s%${s##*[![:space:]]}}"
  printf '%s' "$s"
}

field_value() {
  local file="$1"
  local label="$2"
  local line
  line="$(grep -m1 -E "^- ${label}:" "$file" || true)"
  if [[ -z "$line" ]]; then
    printf ''
    return 0
  fi
  printf '%s' "${line#- ${label}: }"
}

has_verified_against_bullets() {
  local file="$1"
  awk '
    BEGIN { in_section=0; saw_header=0; saw_bullet=0 }
    /^- Verified Against:[[:space:]]*$/ { in_section=1; saw_header=1; next }
    in_section == 1 {
      if ($0 ~ /^[[:space:]]+- /) { saw_bullet=1; next }
      if ($0 ~ /^[[:space:]]*$/) { next }
      if ($0 ~ /^## / || $0 ~ /^# / || $0 ~ /^- [A-Za-z]/ || $0 ~ /^[^[:space:]]/) { in_section=0 }
    }
    END {
      if (saw_header == 1 && saw_bullet == 1) { exit 0 }
      exit 1
    }
  ' "$file"
}

validate_file() {
  local file="$1"
  local ok=1

  if [[ ! -f "$file" ]]; then
    printf 'FAIL  %s: file not found\n' "$file"
    return 1
  fi

  if ! grep -q '^## Document Metadata$' "$file"; then
    printf 'FAIL  %s: missing "## Document Metadata" section\n' "$file"
    ok=0
  fi

  local doc_type
  doc_type="$(field_value "$file" "Document Type")"
  case "$doc_type" in
    "Reference Standard"|"Architecture Standard"|"Operational Runbook") ;;
    *)
      printf 'FAIL  %s: invalid Document Type "%s"\n' "$file" "$doc_type"
      ok=0
      ;;
  esac

  local audience
  audience="$(field_value "$file" "Audience")"
  if [[ -z "$audience" ]]; then
    printf 'FAIL  %s: missing Audience\n' "$file"
    ok=0
  else
    IFS=',' read -r -a audience_items <<< "$audience"
    for item in "${audience_items[@]}"; do
      local t
      t="$(trim "$item")"
      case "$t" in
        "Engineer"|"SRE"|"Security"|"Architect") ;;
        *)
          printf 'FAIL  %s: invalid Audience token "%s"\n' "$file" "$t"
          ok=0
          ;;
      esac
    done
  fi

  local stability
  stability="$(field_value "$file" "Stability")"
  case "$stability" in
    "Stable"|"Evolving"|"Experimental") ;;
    *)
      printf 'FAIL  %s: invalid Stability "%s"\n' "$file" "$stability"
      ok=0
      ;;
  esac

  local verified_date
  verified_date="$(field_value "$file" "Last Verified")"
  if [[ ! "$verified_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    printf 'FAIL  %s: Last Verified must match YYYY-MM-DD\n' "$file"
    ok=0
  fi

  if ! has_verified_against_bullets "$file"; then
    printf 'FAIL  %s: Verified Against must include at least one bullet item\n' "$file"
    ok=0
  fi

  if [[ "$ok" -eq 1 ]]; then
    printf 'PASS  %s\n' "$file"
    return 0
  fi

  return 1
}

for file in "${files[@]}"; do
  if ! validate_file "$file"; then
    fail_count=$((fail_count + 1))
  fi
done

if [[ "$fail_count" -gt 0 ]]; then
  printf '\nMetadata validation failed for %d file(s).\n' "$fail_count"
  exit 1
fi

printf '\nMetadata validation passed for all official documentation pages.\n'
