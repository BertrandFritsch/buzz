//! Retain job options while process-wrap temporarily removes its wrapper map.

use std::io::Result;

use process_wrap::tokio::{ChildWrapper, CommandWrap, CommandWrapper, JobObject, KillOnDrop};
use tokio::process::Command;
use windows::Win32::System::Threading::{CREATE_NO_WINDOW, CREATE_SUSPENDED};

#[derive(Clone, Copy, Debug)]
pub(super) struct WindowsJob;

fn options() -> CommandWrap {
    let mut options = CommandWrap::with_new("", |_| {});
    options.wrap(KillOnDrop);
    options
}

impl CommandWrapper for WindowsJob {
    fn pre_spawn(&mut self, command: &mut Command, _core: &CommandWrap) -> Result<()> {
        // process-wrap9.1 spawn_with moves wrappers out of core before invoking
        // hooks. Supply the safe JobObject wrapper a populated options core so
        // it retains CREATE_NO_WINDOW and kernel KILL_ON_JOB_CLOSE.
        JobObject.pre_spawn(command, &options())?;
        // Its get_wrap also downcasts Box<dyn Wrapper> rather than the inner
        // wrapper. Set flags through Tokio's safe API and omit CreationFlags
        // from this option core to avoid that dependency panic.
        command.creation_flags((CREATE_SUSPENDED | CREATE_NO_WINDOW).0);
        Ok(())
    }

    fn wrap_child(
        &mut self,
        child: Box<dyn ChildWrapper>,
        _core: &CommandWrap,
    ) -> Result<Box<dyn ChildWrapper>> {
        JobObject.wrap_child(child, &options())
    }
}
