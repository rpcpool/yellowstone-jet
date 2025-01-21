use {
    std::{collections::BTreeMap, future::Future},
    tokio::{
        runtime::Runtime,
        sync::{
            broadcast,
            mpsc::{self, UnboundedSender},
            oneshot,
        },
        task::{JoinError, JoinHandle},
    },
};

struct Task {
    task_name: String,
    rt: tokio::runtime::Handle,
    callback_to_parent_tx: UnboundedSender<String>,
    parent_dead_rx: broadcast::Receiver<()>,
}

impl Drop for Task {
    fn drop(&mut self) {
        let _ = self.callback_to_parent_tx.send(self.task_name.clone());
    }
}

impl Task {
    async fn scope<F, Fut>(mut self, f: F) -> Result<(), JoinError>
    where
        F: FnOnce(oneshot::Receiver<()>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<()>();
        let mut h = self.rt.spawn(f(rx));
        tokio::select! {
            result = &mut h => { result },
            _result = self.parent_dead_rx.recv() => {
                let _ = tx.send(());
                h.await
            },
        }
    }
}
///
/// Holds multiple tasks and shutdown gracefully all other tasks when one task finished or panic.
///
pub struct TaskGroup {
    rt: tokio::runtime::Handle,
    task_map: BTreeMap<String, JoinHandle<Result<(), JoinError>>>,
    // If the TaskGroup ever gets dropped, this will be triggered to stop all tasks.
    broadcast_stop_tx: broadcast::Sender<()>,

    // A Task get dropped, it will send a message to this channel to notify the TaskGroup to stop.
    callback_to_parent_tx: mpsc::UnboundedSender<String>,
    callback_to_parent_rx: mpsc::UnboundedReceiver<String>,
}

impl Default for TaskGroup {
    fn default() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            rt: tokio::runtime::Handle::current(),
            task_map: BTreeMap::default(),
            broadcast_stop_tx: broadcast::channel(1).0,
            callback_to_parent_tx: tx,
            callback_to_parent_rx: rx,
        }
    }
}

impl TaskGroup {
    pub fn with_runtime(rt: Runtime) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            rt: rt.handle().clone(),
            task_map: BTreeMap::default(),
            broadcast_stop_tx: broadcast::channel(1).0,
            callback_to_parent_tx: tx,
            callback_to_parent_rx: rx,
        }
    }

    ///
    /// Spawn a task that can be canceled safely.
    ///
    pub fn spawn_cancelable<N, F>(&mut self, task_name: N, fut: F)
    where
        N: AsRef<str>,
        F: Future<Output = ()> + Send + 'static,
    {
        let func = move |mut stop_rx| async move {
            tokio::select! {
                _ = fut => {},
                _ = &mut stop_rx => {},
            }
        };

        self.spawn_with_shutdown(task_name, func);
    }

    ///
    /// Spawn a task that accepts a shutdown signal as input so that it can stop itself gracefully.
    ///
    pub fn spawn_with_shutdown<N, F, Fut>(&mut self, task_name: N, f: F)
    where
        F: FnOnce(oneshot::Receiver<()>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
        N: AsRef<str>,
    {
        let task_name = task_name.as_ref().to_string();
        let task_name2 = task_name.clone();
        let stop_rx = self.broadcast_stop_tx.subscribe();
        let callback_to_parent_tx = self.callback_to_parent_tx.clone();
        let handle = self.rt.spawn(async move {
            let task = Task {
                task_name: task_name2,
                rt: tokio::runtime::Handle::current(),
                callback_to_parent_tx,
                parent_dead_rx: stop_rx,
            };

            task.scope(f).await
        });
        self.task_map.insert(task_name, handle);
    }

    pub async fn wait_one(
        mut self,
    ) -> Option<(
        String,
        Result<(), JoinError>,
        BTreeMap<String, Result<(), JoinError>>,
    )> {
        if self.task_map.is_empty() {
            return None;
        }
        let first_task_name = self
            .callback_to_parent_rx
            .recv()
            .await
            .expect("callback_to_parent_rx closed");

        let _ = self.broadcast_stop_tx.send(());

        let handle = self
            .task_map
            .remove(&first_task_name)
            .expect("task not found");

        let result = handle.await.expect("task wrapper failed");

        let mut rest = BTreeMap::default();
        while let Some((task_name, handle)) = self.task_map.pop_first() {
            let result = handle.await.expect("task wrapper failed");
            rest.insert(task_name, result);
        }

        Some((first_task_name, result, rest))
    }
}

#[cfg(test)]
mod tests {
    use {super::*, futures::future, mpsc::unbounded_channel, std::collections::BTreeSet};

    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_with_shutdown_should_stop_each_task_gracefully() {
        let mut tg = TaskGroup::default();

        let (spy_tx, mut spy_rx) = unbounded_channel::<&'static str>();

        let spy_tx2 = spy_tx.clone();
        tg.spawn_with_shutdown("task1", |mut stop| async move {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {},
                _ = &mut stop => {},
            }
            let _ = spy_tx2.send("task1");
        });

        let spy_tx2 = spy_tx.clone();
        tg.spawn_with_shutdown("task2", |mut stop| async move {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(2)) => {},
                _ = &mut stop => {},
            }
            let _ = spy_tx2.send("task2");
        });

        let spy_tx2 = spy_tx.clone();
        tg.spawn_with_shutdown("task3", |mut stop| async move {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(3)) => {},
                _ = &mut stop => {},
            }
            let _ = spy_tx2.send("task3");
        });

        let (task_name, result, rest) = tg.wait_one().await.expect("wait_one failed");
        assert_eq!(task_name, "task1");
        drop(spy_tx);

        let mut actual = BTreeSet::new();
        while let Some(task_name) = spy_rx.recv().await {
            actual.insert(task_name);
        }

        assert!(result.is_ok());
        assert!(rest.values().all(|result| result.is_ok()));
        assert_eq!(rest.len(), 2);
        assert_eq!(
            actual,
            ["task1", "task2", "task3"].iter().cloned().collect()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_with_shutdown_should_stop_every_other_task_gracefully_in_case_on_task_panic() {
        let mut tg = TaskGroup::default();

        let (spy_tx, mut spy_rx) = unbounded_channel::<&'static str>();

        tg.spawn_with_shutdown("task1", |_| async move { panic!("task1 panic") });

        let spy_tx2 = spy_tx.clone();
        tg.spawn_with_shutdown("task2", |mut stop| async move {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(2)) => {},
                _ = &mut stop => {},
            }
            let _ = spy_tx2.send("task2");
        });

        let spy_tx2 = spy_tx.clone();
        tg.spawn_with_shutdown("task3", |mut stop| async move {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(3)) => {},
                _ = &mut stop => {},
            }
            let _ = spy_tx2.send("task3");
        });

        let (task_name, result, rest) = tg.wait_one().await.expect("wait_one failed");
        assert_eq!(task_name, "task1");
        drop(spy_tx);

        let mut actual = BTreeSet::new();
        while let Some(task_name) = spy_rx.recv().await {
            actual.insert(task_name);
        }

        assert!(result.is_err());
        assert!(rest.values().all(|result| result.is_ok()));
        assert_eq!(rest.len(), 2);
        assert_eq!(actual, ["task2", "task3"].iter().cloned().collect());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_cancelable_should_stop_every_task_when_one_panic() {
        let mut tg = TaskGroup::default();

        let (spy_tx, mut spy_rx) = unbounded_channel::<&'static str>();

        tg.spawn_cancelable("task1", async move { panic!("task1 panic") });

        let spy_tx2 = spy_tx.clone();
        tg.spawn_with_shutdown("task2", |mut stop| async move {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(2)) => {},
                _ = &mut stop => {},
            }
            let _ = spy_tx2.send("task2");
        });

        let spy_tx2 = spy_tx.clone();
        tg.spawn_with_shutdown("task3", |mut stop| async move {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(3)) => {},
                _ = &mut stop => {},
            }
            let _ = spy_tx2.send("task3");
        });

        let (task_name, result, rest) = tg.wait_one().await.expect("wait_one failed");
        assert_eq!(task_name, "task1");
        drop(spy_tx);

        let mut actual = BTreeSet::new();
        while let Some(task_name) = spy_rx.recv().await {
            actual.insert(task_name);
        }

        assert!(result.is_err());
        assert!(rest.values().all(|result| result.is_ok()));
        assert_eq!(rest.len(), 2);
        assert_eq!(actual, ["task2", "task3"].iter().cloned().collect());
    }

    #[tokio::test]
    async fn it_should_wait_one_with_no_task() {
        let tg = TaskGroup::default();
        assert!(tg.wait_one().await.is_none());
    }

    #[tokio::test]
    async fn wait_one_should_work_even_if_all_tasks_panic() {
        let mut tg = TaskGroup::default();
        for i in 0..10 {
            tg.spawn_cancelable(format!("task{}", i), async move {
                panic!("task{} panic", i);
            });
        }

        let (_, result, rest) = tg.wait_one().await.expect("wait_one failed");
        assert!(result.is_err());
        assert!(rest.values().all(|result| result.is_err()));
        assert_eq!(rest.len(), 9);
    }

    #[tokio::test]
    async fn task_group_should_drop_every_task_on_task_group_drop() {
        let mut tg = TaskGroup::default();
        let mut rx_vec = vec![];
        for i in 0..10 {
            let (tx, rx) = oneshot::channel::<()>();
            tg.spawn_cancelable(format!("task{}", i), async move {
                future::pending::<()>().await;
                drop(tx);
            });
            rx_vec.push(rx);
        }

        drop(tg);

        while let Some(rx) = rx_vec.pop() {
            assert!(rx.await.is_err());
        }
    }
}
