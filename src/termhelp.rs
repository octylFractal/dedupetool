use std::fmt::Display;
use std::time::Duration;

use console::{style, StyledObject};
use indicatif::{HumanCount, ProgressBar, ProgressDrawTarget, ProgressStyle};

/// Shorthand for printing a single displayable object to stderr.
pub fn log_diag<D: Display>(msg: D) {
    eprintln!("{}", msg);
}

pub trait StderrStyle: Display + Sized {
    fn style(self) -> StyledObject<Self> {
        style(self).for_stderr()
    }

    fn success_style(self) -> StyledObject<Self> {
        self.style().green()
    }

    fn error_style(self) -> StyledObject<Self> {
        self.style().red()
    }
}

impl<D: Display> StderrStyle for D {}

pub trait DedupetoolProgressBar {
    fn enable_steady_tick_dedupetool(&self);

    fn with_steady_tick_dedupetool(self) -> Self
    where
        Self: Sized,
    {
        self.enable_steady_tick_dedupetool();
        self
    }

    fn set_style_dedupetool(&self);

    fn with_style_dedupetool(self) -> Self
    where
        Self: Sized,
    {
        self.set_style_dedupetool();
        self
    }

    fn dedupetool_spinner(item_name: &str) -> Self;
}

impl DedupetoolProgressBar for ProgressBar {
    fn enable_steady_tick_dedupetool(&self) {
        self.enable_steady_tick(Duration::from_millis(75));
    }

    fn set_style_dedupetool(&self) {
        let pos_arg = self
            .length()
            .map(|l| HumanCount(l).to_string().len())
            .map(|len| format!("{{human_pos:>{}}}", len))
            .unwrap_or_else(|| "{human_pos}".to_string());
        self.set_style(
            ProgressStyle::default_bar()
                .template(&format!(
                    "{{percent:>3}}%[{{bar:60.cyan/blue}}] {}/{{human_len}} {{wide_msg}}",
                    pos_arg
                ))
                .unwrap()
                .progress_chars("#|-"),
        );
    }

    fn dedupetool_spinner(item_name: &str) -> Self {
        let bar = ProgressBar::with_draw_target(None, ProgressDrawTarget::stderr());
        bar.set_style(
            ProgressStyle::default_spinner()
                .template(&format!("{{spinner}} {{msg}}: {{human_pos}} {}", item_name))
                .unwrap(),
        );
        bar
    }
}
