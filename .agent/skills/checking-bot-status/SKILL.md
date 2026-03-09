---
name: checking-bot-status
description: Evaluates if the gatebot is functioning properly by analyzing the funding arbitrage strategy (positive and reverse carry) and reviewing the bot log history for the past 48 hours to generate a performance summary. Use when the user asks to check if the bot is working properly.
---

# Checking Gatebot Status

## When to use this skill
- The user asks to check if the bot is working properly.
- The user asks for a summary of the bot's performance over the last 48 hours.
- The user asks if the funding arbitrage strategy (positive or reverse carry) is executing correctly.

## Workflow
1. **Locate Configuration & Logs:** Read `/Users/elonhsiao/Desktop/antigravity/gatebot/config.yaml` to identify the log file path (usually `logs/bot.log`) and current enabled strategies.
2. **Examine 48-Hour Log History:** Search the last 48 hours of entries in the log file. Look specifically for critical patterns:
    - Successful order placements and position matches.
    - Funding fees collected or paid.
    - Warnings or errors (e.g., API rate limits, insufficient margin, connection drops).
3. **Analyze Strategy Execution:** Verify that `positive_carry.py` and/or `reverse_carry.py` logic is behaving as expected (e.g., matching the dual position perfectly to stay delta-neutral).
4. **Generate Summary:** Synthesize the findings into a clear, concise summary for the user, highlighting the bot's overall health, executed strategies, and any anomalies or areas needing attention.

## Instructions
- Use `grep_search` or read the log file using `view_file` (tailing the end) to extract the most recent logs.
- Pay special attention to keyword patterns like `ERROR`, `WARNING`, `Funding`, `PNL`, `Profit`, `Position`, and `Order` inside `logs/bot.log` and `config.yaml` to ensure metric tracking.
- Only summarize the past 48 hours based on timestamps in the log file to ensure the check is localized to recent activity.
- Present the final summary to the user outlining:
  1. Overall Status (Healthy/Unhealthy)
  2. Number/Types of executed trades (Positive vs. Reverse Carry)
  3. Total Profit and Loss (PNL), detailing funding fees collected vs. fees paid.
  4. Any notable errors or warnings.
