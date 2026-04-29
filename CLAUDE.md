do not be agreeable, instead by a scientist: find all possible hypotheses and test them. Do not jump to conclusion but question your own logic.
Consult with prior literature, but this requires understanding the WHYs in papers properly. Do not take my word as gospel, I give your hints,
but I trust you to always deeply think through what I told you and ask yourself why did I tell you what. 

When impl or refactoring always have the long term picture in mind. Most important of all: Correctness. Always ensure correctness. Refactors must preserve the original logic faithfully.
Implementations should consider long term maintainability: Never do quick and dirty, instead ALWAYS adhere to modularity, and the one-concern paradigm.
Never add special casing to code dealing with other concerns, always have new features self-contained and integrated through the modular API.
If the API is insufficient then it must be improved to fit a generalized use case, not the special usage case that you impl at the moment. 

never have dublicated logic or special casing, this is always a sign that you write quick and dirty, not maintainable code. Instead use a proper modular system, so that a single-concern does not even need to know or touch anything of the new logic. the API allows for smooth integration. (e.g. a pipelines or any other modular system). if you encouter code that violates any of this please flag this to the user. If it is code you just wrote, fix it immidieately instead.

Before writing any implementation code, write down in a comment or scratchpad: (1) which module owns this concern, (2) what the API surface crossing that boundary looks like, (3) what the callers see. If step 2 requires callers to know anything about the internals of 
the owned module, stop — the API is wrong. Only proceed once the boundary is clean. 

The antipattern is easy:
  - "If statements" if your code adds if statements this nearly in all cases proves that you are breaking the rules and do not write maintainable code concerving modularity and single-concern.
  - A function whose name contains two different domain nouns (e.g. normalize_embedding_weights living in an executor file)
  - The same import appearing in N≥2 files at the same call site level

Never write code again from memory, instead always use the cp and move command to faithfully get the code to the new location, and then edit the copied file to remove anything that was not needed or adjust.

Force the design-first step:
Never open an implementation file until you can state in one sentence what the single concern of the change is and which module boundary it crosses. If the sentence contains "and", the design is wrong.

when performing renaming or refactoring, never recreate files from memory, instead with mv/cp the file, then perform surgical edits. You MUST always do this as it avoids unintended misedits that you otherwise do all the time.

Step semantics (post-pipeline cutover): one logged ``step`` is one optimizer step,
uniformly across all subphases (Standard, DualSample, GeoRange, RoundHandoff,
Tercile, NaturalLength). Anything that previously said "step" still does, but the
natural-length subphase no longer reports per-fwd+bwd — gradient-accum minibatches
are silent. Schedule horizons in config (``max_steps``, ``checkpoint_interval``,
``log_interval``, ``exit_loss_window``, ``tau_schedule_steps``,
``spread_trajectory_steps``) are in optimizer steps. Old natural-length checkpoints
are auto-rescaled by 1/ga on resume — see ``_orchestrator.py`` and
``SpecTrainingState.pipeline_v1``.

rules in .rules
