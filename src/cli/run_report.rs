//! A plain-English account of an ingest run, printed to stderr when it fails.
//!
//! Cron mails stderr, and the web repo's `cron/cron-mail.sh` turns it into the
//! email: the `SUMMARY:` line becomes the subject, `IMPACT:` lines say what
//! users see, and the step table is the body. Before this the mail was one
//! Rust error chain, which did not say that prices had still updated or that
//! the next hourly run would retry - so a routine dropped download read the
//! same as an outage.

use std::fmt::Write;

#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum Step {
    Reset,
    Sets,
    CardsAndSealed,
    Cards,
    Sealed,
    SetCards,
    Prices,
    CardKingdom,
    Prune,
    Updates,
    RecordCompletion,
}

impl Step {
    fn label(self) -> &'static str {
        match self {
            Step::Reset => "Reset all data",
            Step::Sets => "Sets",
            Step::CardsAndSealed => "Cards and sealed products (AllPrintings.json)",
            Step::Cards => "Cards (AllPrintings.json)",
            Step::Sealed => "Sealed products (AllPrintings.json)",
            Step::SetCards => "Cards for one set",
            Step::Prices => "Prices (AllPricesToday.json)",
            Step::CardKingdom => "Card Kingdom buylist",
            Step::Prune => "Cleanup of unwanted cards and sets",
            Step::Updates => "Set sizes and set totals",
            Step::RecordCompletion => "Marking the update complete",
        }
    }

    /// What went wrong, as the subject line would say it.
    fn headline(self) -> &'static str {
        match self {
            Step::Reset => "data reset failed",
            Step::Sets => "set list not updated",
            Step::CardsAndSealed | Step::Cards | Step::SetCards => "card catalog not updated",
            Step::Sealed => "sealed products not updated",
            Step::Prices => "PRICES NOT UPDATED",
            Step::CardKingdom => "Card Kingdom buylist not updated",
            Step::Prune | Step::Updates => "post-update cleanup failed",
            Step::RecordCompletion => "update not marked complete",
        }
    }

    /// What a user of the site would notice.
    fn impact(self) -> &'static str {
        match self {
            Step::Reset => "The data may be partly deleted.",
            Step::Sets => "Newly released sets may be missing.",
            Step::CardsAndSealed | Step::Cards | Step::SetCards => {
                "New cards and corrections from MTGJSON's latest build are not on the site yet."
            }
            Step::Sealed => "New sealed products from MTGJSON's latest build are missing.",
            Step::Prices => "Prices on the site are still from the previous update.",
            Step::CardKingdom => "Card Kingdom buylist offers are still from the previous update.",
            Step::Prune | Step::Updates => "Set sizes and set value totals may be off.",
            Step::RecordCompletion => "Nothing visible; the next hourly run will redo the update.",
        }
    }
}

#[derive(Debug)]
enum Outcome {
    Ok,
    Failed(String),
    Skipped(&'static str),
}

#[derive(Default)]
pub(crate) struct RunReport {
    steps: Vec<(Step, Outcome)>,
}

impl RunReport {
    pub fn ok(&mut self, step: Step) {
        self.steps.push((step, Outcome::Ok));
    }

    pub fn failed(&mut self, step: Step, err: &anyhow::Error) {
        self.steps.push((step, Outcome::Failed(format!("{err:#}"))));
    }

    pub fn skipped(&mut self, step: Step, why: &'static str) {
        self.steps.push((step, Outcome::Skipped(why)));
    }

    fn step_ok(&self, step: Step) -> bool {
        self.steps
            .iter()
            .any(|(s, o)| *s == step && matches!(o, Outcome::Ok))
    }

    pub fn render(&self) -> String {
        let mut failed: Vec<Step> = Vec::new();
        for (step, outcome) in &self.steps {
            if matches!(outcome, Outcome::Failed(_)) && !failed.contains(step) {
                failed.push(*step);
            }
        }
        // Prices lead: they are what the site is for.
        failed.sort_by_key(|s| *s != Step::Prices);
        let mut headlines: Vec<&str> = Vec::new();
        let mut impacts: Vec<&str> = Vec::new();
        for step in &failed {
            if !headlines.contains(&step.headline()) {
                headlines.push(step.headline());
            }
            if !impacts.contains(&step.impact()) {
                impacts.push(step.impact());
            }
        }
        let mut summary = if headlines.is_empty() {
            "the update did not finish".to_string()
        } else {
            headlines.join(", ")
        };
        if !failed.contains(&Step::Prices) && self.step_ok(Step::Prices) {
            summary.push_str(" (prices OK)");
        }
        let mut out = format!("SUMMARY: {}\n", capitalize(&summary));
        for impact in impacts {
            let _ = writeln!(out, "IMPACT: {impact}");
        }
        for (step, outcome) in &self.steps {
            match outcome {
                Outcome::Ok => {
                    let _ = writeln!(out, "  OK       {}", step.label());
                }
                Outcome::Failed(err) => {
                    let _ = writeln!(out, "  FAILED   {}", step.label());
                    let _ = writeln!(out, "           {err}");
                }
                Outcome::Skipped(why) => {
                    let _ = writeln!(out, "  SKIPPED  {}: {why}", step.label());
                }
            }
        }
        out
    }
}

fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failed_card_ingest_says_prices_still_updated() {
        let mut report = RunReport::default();
        report.ok(Step::Sets);
        report.failed(
            Step::CardsAndSealed,
            &anyhow::anyhow!("connection reset by peer"),
        );
        report.ok(Step::Prices);
        report.ok(Step::CardKingdom);
        report.skipped(Step::Prune, "it runs only after a complete ingest");

        let text = report.render();

        assert!(text.starts_with("SUMMARY: Card catalog not updated (prices OK)\n"));
        assert!(text.contains("IMPACT: New cards and corrections"));
        assert!(text.contains(
            "  FAILED   Cards and sealed products (AllPrintings.json)\n           connection reset by peer\n"
        ));
        assert!(text.contains("  OK       Prices (AllPricesToday.json)\n"));
        assert!(text.contains("  SKIPPED  Cleanup of unwanted cards and sets: it runs only"));
    }

    #[test]
    fn a_price_failure_leads_the_summary() {
        let mut report = RunReport::default();
        report.failed(Step::Sets, &anyhow::anyhow!("boom"));
        report.failed(Step::Prices, &anyhow::anyhow!("boom"));

        let text = report.render();

        assert!(text.starts_with("SUMMARY: PRICES NOT UPDATED, set list not updated\n"));
        assert!(!text.contains("prices OK"));
    }

    #[test]
    fn steps_sharing_a_headline_are_named_once() {
        let mut report = RunReport::default();
        report.failed(Step::Prune, &anyhow::anyhow!("a"));
        report.failed(Step::Updates, &anyhow::anyhow!("b"));

        let text = report.render();

        assert!(text.starts_with("SUMMARY: Post-update cleanup failed\n"));
        assert_eq!(text.matches("IMPACT:").count(), 1);
    }
}
