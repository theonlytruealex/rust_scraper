use std::{sync::Arc, time::Duration};

use polars::{df, frame::DataFrame};
use thirtyfour::prelude::*;
use clap::Parser;
use tokio::time::sleep;
use polars_io::prelude::*;
use std::fs::File;
use regex::Regex;


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    place: String,

    #[arg(short, long, default_value_t = 1)]
    count: u8,

    #[arg(long, short)]
    start_date: Option<String>,

    #[arg(long, short)]
    end_date: Option<String>,
}

#[tokio::main]
async fn main() -> WebDriverResult<()> {
    let args = Args::parse();
    let caps = DesiredCapabilities::chrome();
    let driver = WebDriver::new("http://localhost:4444", caps).await.expect("failed to connect to WebDriver");

    let place = Arc::new(args.place);
    let place1 = Arc::clone(&place);
    let count = args.count;

    let dates = match (args.start_date, args.end_date) {
        (Some(start), Some(end)) => Some((start, end)),
        (Some(_), None) => None,
        (None, Some(_)) => None,
        _ => None,
    };

    let airbnb_dates = dates.clone();
    let airbnb = tokio::spawn(async move{
        airbnb_scrape(place1, driver, count, airbnb_dates).await.expect("Failed to scrape Airbnb");
    });

    airbnb.await.expect("Failed Airbnb");
    Ok(())
}

async fn airbnb_scrape(place_arc: Arc<String>, driver: WebDriver, mut count: u8, dates: Option<(String, String)>) -> WebDriverResult<()>{
    let place = place_arc.to_string();
    driver.maximize_window().await?;
    driver.goto("https://www.airbnb.ie/?locale=en&_set_bev_on_new_domain=1738317976_EAODY3MmFhMGFkYz").await?;

    let url = driver.current_url().await?;
    assert_eq!(url.as_ref(), "https://www.airbnb.ie/?locale=en&_set_bev_on_new_domain=1738317976_EAODY3MmFhMGFkYz");
    sleep(Duration::from_secs(5)).await;

    click_button(&driver, "#react-application > div > div > div:nth-child(1) > div > div.c5vrlhl.atm_mk_1n9t6rb.atm_fq_idpfg4.atm_vy_1osqo2v.atm_wq_z68epy.atm_lk_1ph3nq8.atm_ll_1ph3nq8.sbhok83.atm_p_1wv4lnm.atm_tw_1tftv22.atm_ubcqrc_ccgtyg.atm_p_p3f3nu__1rrf6b5.dir.dir-ltr > section > div > div.mv91188.atm_9s_1txwivl.atm_ar_1bp4okc.atm_gi_1yuitx.atm_h3_8tjzot.atm_h3_1yuitx__oggzyc.atm_ar_vrvcex__oggzyc.dir.dir-ltr > div:nth-child(2) > button".to_owned())
                .await;

    let search = driver.find(By::Css("#bigsearch-query-location-input")).await.expect("failed to locate search box");
    search.wait_until().clickable().await.expect("Search not clickable");
    search.send_keys(place).await.expect("Failed to search for place");

    if count > 1 { 
        click_button(&driver, "#search-tabpanel > div > div.c111bvlt.atm_9s_1txwivl.atm_1eltean_1osqo2v.c1gh7ier.atm_am_1qhqiko.dir.dir-ltr > div.c1ddhymz.atm_h_1h6ojuz.atm_9s_1txwivl.atm_gi_1n1ank9.atm_jb_idpfg4.atm_mk_h2mmj6.atm_vy_10bmcub.cggll98.atm_am_1qhqiko.dir.dir-ltr > div.b1spesa7.atm_1s_glywfm.atm_26_1j28jx2.atm_3f_idpfg4.atm_7l_1kw7nm4.atm_9j_tlke0l.atm_bx_1kw7nm4.atm_c8_1kw7nm4.atm_cs_1kw7nm4.atm_g3_1kw7nm4.atm_gi_idpfg4.atm_ks_ewfl5b.atm_rd_glywfm.atm_vb_1wugsn5.atm_kd_glywfm.atm_9s_1ulexfb.atm_am_qk3dho.atm_l8_t94yts.atm_r3_1e5hqsa.atm_vy_idpfg4.atm_wq_kb7nvz.atm_3f_glywfm_jo46a5.atm_l8_idpfg4_jo46a5.atm_gi_idpfg4_jo46a5.atm_3f_glywfm_1icshfk.atm_kd_glywfm_19774hq.atm_6h_1s2714j_vmtskl.atm_66_nqa18y_vmtskl.atm_4b_1egtlkw_vmtskl.atm_92_1yyfdc7_vmtskl.atm_9s_glywfm_vmtskl.atm_e2_1vi7ecw_vmtskl.atm_fq_idpfg4_vmtskl.atm_h3_4h84z3_vmtskl.atm_mk_stnw88_vmtskl.atm_n3_idpfg4_vmtskl.atm_tk_1ssbidh_vmtskl.atm_wq_idpfg4_vmtskl.atm_2a_1u8qnfj_9in345.atm_3f_okh77k_9in345.atm_5j_1vi7ecw_9in345.atm_6i_idpfg4_9in345.atm_92_1yyfdc7_9in345.atm_fq_idpfg4_9in345.atm_mk_stnw88_9in345.atm_n3_idpfg4_9in345.atm_tk_idpfg4_9in345.atm_wq_idpfg4_9in345.b1fbhdca.atm_9s_1ulexfb_1rqz0hn.atm_gi_eflcwz_9bj8xt.atm_2d_um1unu_9bj8xt.atm_wq_cs5v99_1w3cfyq.atm_9s_1ulexfb_9xuho3.atm_uc_aaiy6o_1tasb51.atm_4b_dezgoh_1tasb51.atm_70_1t2bbnk_1tasb51.atm_gi_eflcwz_1tasb51.atm_uc_glywfm_1tasb51_1rrf6b5.atm_wq_cs5v99_pfnrn2_1oszvuo.atm_9s_1ulexfb_1buez3b_1oszvuo.atm_uc_aaiy6o_1fu4lp4_1oszvuo.atm_4b_dezgoh_1fu4lp4_1oszvuo.atm_70_1t2bbnk_1fu4lp4_1oszvuo.atm_gi_eflcwz_1fu4lp4_1oszvuo.atm_uc_glywfm_1fu4lp4_1o31aam.b1889vka.atm_5q_idpfg4_agv9cz.atm_h0_idpfg4_1ve49u.dir.dir-ltr".to_owned())
                    .await;
    }
    while count > 1 {
        click_button(&driver, "#stepper-adults > button:nth-child(3)".to_owned()).await;
        count -= 1;
    }

    if let Some((start, end)) = dates {
        get_dates(&driver, start, end).await;
    }
    let submit = driver.find(By::Css("#search-tabpanel > div > div.c111bvlt.atm_9s_1txwivl.atm_1eltean_1osqo2v.c1gh7ier.atm_am_1qhqiko.dir.dir-ltr > div.c1ddhymz.atm_h_1h6ojuz.atm_9s_1txwivl.atm_gi_1n1ank9.atm_jb_idpfg4.atm_mk_h2mmj6.atm_vy_10bmcub.cggll98.atm_am_1qhqiko.dir.dir-ltr > div.snd2ne0.atm_am_12336oc.atm_gz_yjp0fh.atm_ll_rdoju8.atm_mk_h2mmj6.atm_wq_qfx8er.dir.dir-ltr > button"))
                                        .await.expect("Could not find submit button");
    submit.click().await.expect("Could not click button");
    sleep(Duration::from_secs(2)).await;  

    let mut df: DataFrame = extract_specific_children(&driver).await.unwrap();
    let mut distances: Vec<i32> = Vec::with_capacity(df["Link"].len());

    let mut file = File::create("results.csv").expect("could not create file");
    CsvWriter::new(&mut file)
        .include_header(true)
        .with_separator(b',')
        .finish(&mut df)
        .unwrap();


    sleep(Duration::from_secs(100)).await;
    Ok(())
}

async fn get_dates(driver: &WebDriver,start: String, end: String) {
    click_button(&driver, "#search-tabpanel > div > div.cwk1mic.atm_9s_1txwivl.atm_am_eqk4pz.atm_jb_idpfg4.dir.dir-ltr > div:nth-child(1) > div".to_owned()).await;
    sleep(Duration::from_millis(500)).await;  
    let start_css = format!("[data-state--date-string=\"{}\"]", start);

    loop {
        match driver.find(By::Css(start_css.clone())).await {
        Ok(button) => {
            button.click().await.unwrap();
            break;
        }
        Err(_) => {
            click_button(&driver, "#panel--tabs--0 > div > div.h1yby4o2.atm_9s_1o8liyq.atm_mk_h2mmj6.h12enwjs.atm_vy_1a0j60c.atm_gz_vgba9w.atm_h0_vgba9w.dir.dir-ltr > div.cso065s.atm_mk_stnw88.atm_tk_1ou6n1d.atm_vy_1osqo2v.atm_9s_1txwivl.atm_fc_1yb4nlp.atm_l8_1chqs9b.dir.dir-ltr > button:nth-child(2)".to_owned()).await;
            sleep(Duration::from_millis(500)).await;
        }
        }
    }
    let end_css = format!("[data-state--date-string=\"{}\"]", end);
    sleep(Duration::from_secs(1)).await;

    loop {
        match driver.find(By::Css(end_css.clone())).await {
            Ok(button) => {
                button.click().await.unwrap();
                break;
            }
            Err(_) => {
                click_button(&driver, "#panel--tabs--0 > div > div.h1yby4o2.atm_9s_1o8liyq.atm_mk_h2mmj6.h12enwjs.atm_vy_1a0j60c.atm_gz_vgba9w.atm_h0_vgba9w.dir.dir-ltr > div.cso065s.atm_mk_stnw88.atm_tk_1ou6n1d.atm_vy_1osqo2v.atm_9s_1txwivl.atm_fc_1yb4nlp.atm_l8_1chqs9b.dir.dir-ltr > button:nth-child(2)".to_owned()).await;
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn click_button(driver: &WebDriver, id: String) {
    let cookies:Result<WebElement, WebDriverError> = match driver.find(By::Css(id)).await {
        Ok(element) => {Ok(element)},
        Err(error) => {Err(error)},
    };
    match cookies {
        Ok(cookie) => {cookie.click().await.unwrap();},
        Err(_) => {},
    };
}

async fn extract_specific_children(driver: &WebDriver) -> WebDriverResult<DataFrame> {
    let mut prices: Vec<i32> = Vec::new();
    let mut ratings:Vec<f32> = Vec::new();
    let mut rating_counts: Vec<i32> = Vec::new();
    let mut links:Vec<String> = Vec::new();
    let elements = driver.find_all(By::Css(".c4mnd7m.atm_9s_11p5wf0.atm_dz_1osqo2v.dir.dir-ltr")).await.unwrap();
    
    loop 
    {
        sleep(Duration::from_secs(1)).await;
        for element in elements.iter() {
            let mut no_rating = false;
            if let Ok(rating) = element.find(By::Css("span[class='a8jt5op atm_3f_idpfg4 atm_7h_hxbz6r atm_7i_ysn8ba atm_e2_t94yts atm_ks_zryt35 atm_l8_idpfg4 atm_vv_1q9ccgz atm_vy_t94yts au0q88m atm_mk_stnw88 atm_tk_idpfg4 dir dir-ltr']")).await {
                if let Ok(text) = rating.text().await {
                    match extract_rating_info(text.trim()) {
                        Some((rating,count)) => {
                            ratings.push(rating);
                            rating_counts.push(count);
                        }
                        None => {
                            ratings.push(0.0);
                            rating_counts.push(0);
                        }
                    }
                }
            } else {
                no_rating = true
            }

            if let Ok(price) = element.find(By::Css("div[class='_tt122m']")).await {
                if let Ok(text) = price.text().await {
                    match extract_price(text.trim()) {
                        Some(price) => {
                            prices.push(price);
                        }
                        None => {
                            prices.push(999999);
                        }
                    }
                    if no_rating {
                        ratings.push(0.0);
                        rating_counts.push(0);
                    }
                }
            }

            if let Ok(link) = element.find(By::Css("a")).await {
                if let Ok(Some(url)) = link.attr("href").await {
                    links.push(url);
                }
            }
        }
        match driver.find(By::Css("a[class='l1ovpqvx atm_1he2i46_1k8pnbi_10saat9 atm_yxpdqi_1pv6nv4_10saat9 atm_1a0hdzc_w1h1e8_10saat9 atm_2bu6ew_929bqk_10saat9 atm_12oyo1u_73u7pn_10saat9 atm_fiaz40_1etamxe_10saat9 c1ytbx3a atm_mk_h2mmj6 atm_9s_1txwivl atm_h_1h6ojuz atm_fc_1h6ojuz atm_bb_idpfg4 atm_26_1j28jx2 atm_3f_glywfm atm_7l_hkljqm atm_gi_idpfg4 atm_l8_idpfg4 atm_uc_10d7vwn atm_kd_glywfm atm_gz_8tjzot atm_uc_glywfm__1rrf6b5 atm_26_zbnr2t_1rqz0hn_uv4tnr atm_tr_kv3y6q_csw3t1 atm_26_zbnr2t_1ul2smo atm_3f_glywfm_jo46a5 atm_l8_idpfg4_jo46a5 atm_gi_idpfg4_jo46a5 atm_3f_glywfm_1icshfk atm_kd_glywfm_19774hq atm_70_glywfm_1w3cfyq atm_uc_aaiy6o_9xuho3 atm_70_18bflhl_9xuho3 atm_26_zbnr2t_9xuho3 atm_uc_glywfm_9xuho3_1rrf6b5 atm_70_glywfm_pfnrn2_1oszvuo atm_uc_aaiy6o_1buez3b_1oszvuo atm_70_18bflhl_1buez3b_1oszvuo atm_26_zbnr2t_1buez3b_1oszvuo atm_uc_glywfm_1buez3b_1o31aam atm_7l_1wxwdr3_1o5j5ji atm_9j_13gfvf7_1o5j5ji atm_26_1j28jx2_154oz7f atm_92_1yyfdc7_vmtskl atm_9s_1ulexfb_vmtskl atm_mk_stnw88_vmtskl atm_tk_1ssbidh_vmtskl atm_fq_1ssbidh_vmtskl atm_tr_pryxvc_vmtskl atm_vy_1vi7ecw_vmtskl atm_e2_1vi7ecw_vmtskl atm_5j_1ssbidh_vmtskl atm_mk_h2mmj6_1ko0jae dir dir-ltr']")).await {
            Ok(button) => button.click().await?,
            Err(_) => break,
        };

    }
    let df = df![
        "Price" => prices,
        "Rating" => ratings,
        "Rating Counts" => rating_counts,
        "Link" => links
    ].unwrap();

    Ok(df)
}

fn extract_rating_info(text: &str) -> Option<(f32, i32)> {
    let re = Regex::new(r"([\d.]+).*rating.*\s(\d+)\s").unwrap();
    
    if let Some(caps) = re.captures(text) {
        let rating: f32 = caps.get(1)?.as_str().parse().ok()?;
        let reviews: i32 = caps.get(2)?.as_str().parse().ok()?;
        return Some((rating, reviews));
    }
    
    None
}


fn extract_price(text: &str) -> Option<i32> {
    let re = Regex::new(r"([\d.]+)").unwrap();

    if let Some(caps) = re.captures(text) {
        let price: i32 = caps[1].parse().ok()?;
        return Some(price);
    }
    
    None
}
