//! 临时：加载 td/md 替身、完成握手并常驻，供 ctp-mock-ctl 端到端驱动（任务8验证）。
use std::time::Duration;

fn main() {
    unsafe { std::env::set_var("CTP_MOCK_AUTO_TICK", "1") };
    use ctp2rs::v1alpha1::{MdApi, TraderApi};
    let td_so = std::env::var("TD_SO").unwrap();
    let md_so = std::env::var("MD_SO").unwrap();
    let td = TraderApi::create_api(&td_so, "./ctl_td_flow_", false);
    td.register_front("tcp://127.0.0.1:40001");
    td.init();
    let md = MdApi::create_api(&md_so, "./ctl_md_flow_", false, false, false);
    md.register_front("tcp://127.0.0.1:42213");
    md.init();
    std::thread::sleep(Duration::from_millis(200));
    println!("READY");
    // 常驻直到被杀；控制服务器在后台线程。
    loop {
        std::thread::sleep(Duration::from_secs(60));
    }
}
