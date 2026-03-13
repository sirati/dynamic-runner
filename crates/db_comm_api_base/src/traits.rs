use crate::command::{Command, Response};

pub trait CommandSender {
    fn send_command(
        &mut self,
        command: Command,
    ) -> impl std::future::Future<Output = Result<(), String>>;
}

pub trait CommandReceiver {
    fn recv_command(&mut self) -> impl std::future::Future<Output = Option<Command>>;
}

pub trait ResponseSender {
    fn send_response(
        &mut self,
        response: Response,
    ) -> impl std::future::Future<Output = Result<(), String>>;
}

pub trait ResponseReceiver {
    fn recv_responses(&mut self) -> impl std::future::Future<Output = Vec<Response>>;
}

pub trait ManagerEndpoint: CommandSender + ResponseReceiver {}
pub trait RunnerEndpoint: CommandReceiver + ResponseSender {}

impl<T: CommandSender + ResponseReceiver> ManagerEndpoint for T {}
impl<T: CommandReceiver + ResponseSender> RunnerEndpoint for T {}
