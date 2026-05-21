import asyncio
import logging

from app.provider_builder import build_provider
from app.review import build_review_context, review_finding
from common.config import get_config
from notifier.utils import notify_about_review

config = get_config()


async def main() -> None:
    """Review changed code and notify only on business-logic findings."""
    review_context = build_review_context()
    if not review_context:
        logging.warning("No review context found; skipping review.")
        return

    model = build_provider(
        ai_provider=config.ai_provider,
        context="",
        ollama_type=config.ai_provider_type,
    )
    review_output = model.review_changes(review_context)
    logging.info("Review model output:\n%s", review_output)
    finding = review_finding(review_output)
    if not finding:
        logging.info("Review completed with no findings.")
        return

    await notify_about_review(finding)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    asyncio.run(main())
