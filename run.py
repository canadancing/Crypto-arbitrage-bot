"""
Multi-Exchange Funding Rate Arbitrage Bot — Entry Point

Run:  python3 run.py
"""

from gateio_funding_arb.multi_bot import MultiExchangeBot


def main() -> None:
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║   Multi-Exchange Funding Rate Arbitrage Bot               ║
    ╚═══════════════════════════════════════════════════════════╝

    ⚠️  RISK WARNING:
    • Trading involves real money and significant risk
    • Start with dry_run: true in config.yaml
    • Check README.md for setup instructions

    To stop: Press Ctrl+C
    """)

    bot = MultiExchangeBot(config_path="config.yaml")
    bot.run()


if __name__ == "__main__":
    main()
