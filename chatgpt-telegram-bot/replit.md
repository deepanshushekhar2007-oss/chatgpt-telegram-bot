# Workspace

## Overview

pnpm workspace monorepo using TypeScript. Each package manages its own dependencies.

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Build**: esbuild (CJS bundle)
- **Telegram Bot**: Grammy v1
- **WhatsApp**: @whiskeysockets/baileys

## Key Commands

- `pnpm run typecheck` — full typecheck across all packages
- `pnpm run build` — typecheck + build all packages
- `pnpm --filter @workspace/api-spec run codegen` — regenerate API hooks and Zod schemas from OpenAPI spec
- `pnpm --filter @workspace/db run push` — push DB schema changes (dev only)
- `pnpm --filter @workspace/api-server run dev` — run API server locally

## Environment Variables

- `TELEGRAM_BOT_TOKEN` — Required to activate the Telegram bot
- `ADMIN_USER_ID` — Telegram user ID of the bot admin
- `FORCE_SUB_CHANNEL` — (Optional) Force users to join a Telegram channel before using the bot
- `SESSION_SECRET` — Session secret
- `WA_SUPPORT_JID` — (Optional) exact WhatsApp Support JID if WhatsApp changes the official support account
- `WA_SUPPORT_PHONE` — (Optional) fallback WhatsApp Support phone number, defaults to `16282000080`

## Bot Features

The Telegram Bot (`artifacts/api-server/src/bot/telegram.ts`) manages WhatsApp groups via Baileys:

1. **Connect WhatsApp** — Phone number pairing code
2. **Create Groups** — Bulk group creation with auto/custom naming
3. **Join Groups** — Bulk join via invite links (with Cancel button)
4. **CTC Checker** — Check contacts in groups via VCF files
5. **Get Link** — Get invite links for admin groups (Similar/All)
6. **Leave Group** — Leave member/admin/all groups
7. **Remove Members** — Remove non-admin members from selected groups
8. **Make Admin** — Promote participants by phone number
9. **Approval** — Approve pending members (1-by-1 or together)
10. **Get Pending List** — See pending member counts per group
11. **Unban Groups** — Select banned groups, start/send the WhatsApp Support chat message with a group-specific appeal prompt, and show live progress with cancel support

See the `pnpm-workspace` skill for workspace structure, TypeScript setup, and package details.
