use crate::opus::OpusStream;
use crate::ytm;
use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::{Decoder, DecoderOptions};
use symphonia::core::errors::Error as DecodeError;
use symphonia::core::formats::{FormatOptions, FormatReader, SeekMode, SeekTo};
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;
use symphonia::core::units::{Time, TimeBase};

const RANGE_BYTES: u64 = 1024 * 1024;
const WINDOW_BYTES: u64 = 8 * 1024 * 1024;
const KEPT_BEHIND: u64 = 1024 * 1024;
const BUFFERED_AHEAD: u64 = WINDOW_BYTES - KEPT_BEHIND - RANGE_BYTES;
const FULL_POLL: Duration = Duration::from_millis(10);
const PAUSED_POLL: Duration = Duration::from_millis(250);
const WINDOW_POLL: Duration = Duration::from_millis(200);
const MAX_STALLS: u32 = 5;
const QUEUED_CHUNKS: usize = 24;
const PRE_ROLL_MS: u64 = 80;
const SILENCE_SAMPLES: usize = 512;
const URL_LIFETIME: Duration = Duration::from_secs(3 * 60 * 60);
const URL_CACHE_ENTRIES: usize = 64;
const SELFTEST_FAR_SEEK_MS: u64 = 20 * 60 * 1000;
const DEVICE_POLL: Duration = Duration::from_secs(2);
const NO_OUTPUT: &str = "No audio output device was found";

type Wake = Arc<dyn Fn() + Send + Sync>;
type Chunks = Arc<Mutex<Receiver<Chunk>>>;
type Urls = Arc<Mutex<HashMap<String, (Instant, ytm::Stream)>>>;

#[derive(Default)]
struct Shared {
    generation: AtomicU64,
    epoch: AtomicU64,
    position_ms: AtomicU64,
    duration_ms: AtomicU64,
    buffered: AtomicU32,
    loading: AtomicBool,
    paused: AtomicBool,
    ended: AtomicBool,
    /// Volume as f32 bits (applied squared). Read by the decode loop when
    /// it (re)opens the output, which the UI thread never touches.
    volume: AtomicU32,
    error: Mutex<Option<String>>,
    current_id: Mutex<String>,
    queue: Mutex<Queue>,
    /// Position to resume from, set once when restoring the saved session.
    /// Consumed by the first play, whoever triggers it (UI or MPRIS).
    resume_at: Mutex<Option<u64>>,
}

/// Snapshot of the UI queue so the decode thread can advance on its own
/// when the UI thread is stalled (a minimised or hidden window stops
/// eframe redraws on Wayland, which used to gate every auto-advance).
/// Full items (not just ids) so background integrations like the MPRIS
/// ticker can resolve titles and covers without the UI thread.
#[derive(Default)]
struct Queue {
    items: Vec<ytm::Item>,
    /// 0 = off, 1 = repeat all, 2 = repeat one.
    repeat: u8,
}

struct Chunk {
    samples: Vec<f32>,
    start_ms: u64,
    epoch: u64,
    rate: u32,
    channels: u16,
    end: bool,
}

enum Cmd {
    Load {
        video_id: String,
        generation: u64,
        epoch: u64,
    },
    Opened(Track),
    Failed {
        generation: u64,
        message: String,
    },
    Seek {
        ms: u64,
        epoch: u64,
    },
    /// Apply the (already stored) pause flag to the loop-owned sink.
    Pause {
        paused: bool,
    },
    /// Apply a volume to the loop-owned sink.
    SetVolume {
        volume: f32,
    },
    /// Recheck the default output device, reopening when it moved.
    PollDevice,
    /// Warm the stream cache for a video id without loading it.
    Preload {
        video_id: String,
    },
}

#[derive(Default)]
struct Window {
    start: u64,
    bytes: VecDeque<u8>,
    reader: u64,
}

impl Window {
    fn end(&self) -> u64 {
        self.start + self.bytes.len() as u64
    }
}

struct Fetch {
    window: Mutex<Window>,
    changed: Condvar,
    len: u64,
    failed: AtomicBool,
    cancelled: AtomicBool,
    generation: u64,
    shared: Arc<Shared>,
}

impl Fetch {
    fn start(yt: &ytm::Client, url: String, len: u64, generation: u64, shared: Arc<Shared>) -> Arc<Fetch> {
        let (agent, user_agent) = (yt.agent.clone(), yt.player_agent());
        let window = Window {
            bytes: VecDeque::with_capacity(len.min(WINDOW_BYTES) as usize),
            ..Window::default()
        };
        let fetch = Arc::new(Fetch {
            window: Mutex::new(window),
            changed: Condvar::new(),
            len,
            failed: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            generation,
            shared,
        });
        let worker = fetch.clone();
        std::thread::spawn(move || {
            let complete = worker.download(&agent, &url, &user_agent);
            worker.failed.store(!complete, Relaxed);
            worker.changed.notify_all();
        });
        fetch
    }

    fn current(&self) -> bool {
        self.shared.generation.load(Relaxed) == self.generation && !self.cancelled.load(Relaxed)
    }

    fn next_range(&self) -> Option<u64> {
        let windowed = self.len > WINDOW_BYTES;
        let mut window = self.window.lock().unwrap();
        while self.current() {
            if windowed {
                if window.reader < window.start || window.reader > window.end() + RANGE_BYTES {
                    window.bytes.clear();
                    window.start = window.reader;
                }
                let played = window
                    .reader
                    .saturating_sub(window.start + KEPT_BEHIND)
                    .min(window.bytes.len() as u64);
                window.bytes.drain(..played as usize);
                window.start += played;
            }
            let end = window.end();
            if end < self.len && (!windowed || end < window.reader + BUFFERED_AHEAD) {
                return Some(end);
            }
            if !windowed {
                return None;
            }
            window = self.changed.wait_timeout(window, WINDOW_POLL).unwrap().0;
        }
        None
    }

    fn download(&self, agent: &ureq::Agent, url: &str, user_agent: &str) -> bool {
        let mut stalls = 0;
        let mut block = [0u8; 16 * 1024];
        while let Some(from) = self.next_range() {
            let mut at = from;
            let to = (from + RANGE_BYTES).min(self.len) - 1;
            let request = agent.get(&format!("{url}&range={from}-{to}")).set("User-Agent", user_agent);
            if let Ok(response) = request.call() {
                let mut body = response.into_reader();
                while let Ok(read) = body.read(&mut block) {
                    if read == 0 || !self.current() {
                        break;
                    }
                    self.window.lock().unwrap().bytes.extend(&block[..read]);
                    at += read as u64;
                    self.changed.notify_all();
                    self.shared.buffered.store((at * 1000 / self.len) as u32, Relaxed);
                }
            }
            if at > from {
                stalls = 0;
                continue;
            }
            stalls += 1;
            if stalls >= MAX_STALLS {
                return false;
            }
            std::thread::sleep(Duration::from_millis(400 * stalls as u64));
        }
        true
    }
}

struct Reader {
    fetch: Arc<Fetch>,
    at: u64,
    seekable: Arc<AtomicBool>,
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.fetch.cancelled.store(true, Relaxed);
        self.fetch.changed.notify_all();
    }
}

impl Read for Reader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let mut window = self.fetch.window.lock().unwrap();
        loop {
            window.reader = self.at;
            if self.at >= window.start && self.at < window.end() {
                let offset = (self.at - window.start) as usize;
                let mut count = 0;
                for (slot, byte) in out.iter_mut().zip(window.bytes.range(offset..)) {
                    *slot = *byte;
                    count += 1;
                }
                self.at += count as u64;
                window.reader = self.at;
                drop(window);
                self.fetch.changed.notify_all();
                return Ok(count);
            }
            if !self.fetch.current() {
                return Err(io::Error::other("cancelled"));
            }
            if self.at >= self.fetch.len {
                return Ok(0);
            }
            if self.fetch.failed.load(Relaxed) {
                return Err(io::Error::other("The connection was lost while streaming"));
            }
            self.fetch.changed.notify_all();
            window = self.fetch.changed.wait_timeout(window, WINDOW_POLL).unwrap().0;
        }
    }
}

impl Seek for Reader {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        let target = match to {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::Current(delta) => self.at as i64 + delta,
            SeekFrom::End(delta) => self.fetch.len as i64 + delta,
        };
        self.at = target.clamp(0, self.fetch.len as i64) as u64;
        Ok(self.at)
    }
}

impl MediaSource for Reader {
    fn is_seekable(&self) -> bool {
        self.seekable.load(Relaxed)
    }

    fn byte_len(&self) -> Option<u64> {
        Some(self.fetch.len)
    }
}

struct Track {
    video_id: String,
    generation: u64,
    format: Box<dyn FormatReader>,
    decoder: Box<dyn Decoder>,
    id: u32,
    time_base: Option<TimeBase>,
    buffer: Option<SampleBuffer<f32>>,
    trim_ms: Option<u64>,
    finished: bool,
}

impl Track {
    fn open(yt: &ytm::Client, urls: &Urls, video_id: &str, generation: u64, shared: &Arc<Shared>) -> Result<Track, String> {
        let mut stream = resolve(yt, urls, video_id)?;
        let fallback = stream.fallback.take();
        Self::open_stream(yt, stream, video_id, generation, shared).or_else(|message| match fallback {
            Some(aac) => Self::open_stream(yt, *aac, video_id, generation, shared),
            None => Err(message),
        })
    }

    fn open_stream(yt: &ytm::Client, stream: ytm::Stream, video_id: &str, generation: u64, shared: &Arc<Shared>) -> Result<Track, String> {
        let ytm::Stream {
            url,
            len,
            duration_ms,
            opus,
            ..
        } = stream;
        if shared.generation.load(Relaxed) == generation {
            shared.duration_ms.store(duration_ms, Relaxed);
        }
        let seekable = Arc::new(AtomicBool::new(false));
        let fetch = Fetch::start(yt, url, len, generation, shared.clone());
        let reader = Reader {
            fetch,
            at: 0,
            seekable: seekable.clone(),
        };
        let source = MediaSourceStream::new(Box::new(reader), Default::default());
        let mut hint = Hint::new();
        hint.with_extension(if opus { "webm" } else { "m4a" });
        let format = symphonia::default::get_probe()
            .format(&hint, source, &FormatOptions::default(), &MetadataOptions::default())
            .map_err(|e| e.to_string())?
            .format;
        seekable.store(true, Relaxed);
        let track = format.default_track().ok_or("This stream has no audio track")?;
        let (id, time_base) = (track.id, track.codec_params.time_base);
        let options = DecoderOptions::default();
        let decoder: Box<dyn Decoder> = if opus {
            Box::new(OpusStream::try_new(&track.codec_params, &options).map_err(|e| e.to_string())?)
        } else {
            symphonia::default::get_codecs()
                .make(&track.codec_params, &options)
                .map_err(|e| e.to_string())?
        };
        Ok(Track {
            video_id: video_id.to_owned(),
            generation,
            format,
            decoder,
            id,
            time_base,
            buffer: None,
            trim_ms: None,
            finished: false,
        })
    }

    fn next_chunk(&mut self, epoch: u64) -> Result<Option<Chunk>, String> {
        loop {
            let packet = match self.format.next_packet() {
                Ok(packet) => packet,
                Err(DecodeError::IoError(e)) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e.to_string()),
            };
            if packet.track_id() != self.id {
                continue;
            }
            let decoded = match self.decoder.decode(&packet) {
                Ok(decoded) => decoded,
                Err(DecodeError::DecodeError(_)) => continue,
                Err(e) => return Err(e.to_string()),
            };
            let spec = *decoded.spec();
            let channels = spec.channels.count();
            let buffer = match &mut self.buffer {
                Some(buffer) if buffer.capacity() >= decoded.frames() * channels => buffer,
                slot => slot.insert(SampleBuffer::new(decoded.frames() as u64, spec)),
            };
            buffer.copy_interleaved_ref(decoded);
            let time = self.time_base.map(|base| base.calc_time(packet.ts()));
            let mut start_ms = time.map_or(0, |t| t.seconds * 1000 + (t.frac * 1000.0) as u64);
            let samples = buffer.samples();
            let mut from = 0;
            if let Some(target) = self.trim_ms {
                let end_ms = start_ms + (samples.len() / channels) as u64 * 1000 / spec.rate as u64;
                if end_ms <= target {
                    continue;
                }
                self.trim_ms = None;
                if start_ms < target {
                    from = ((target - start_ms) * spec.rate as u64 / 1000) as usize * channels;
                    start_ms = target;
                }
            }
            return Ok(Some(Chunk {
                samples: samples[from.min(samples.len())..].to_vec(),
                start_ms,
                epoch,
                rate: spec.rate,
                channels: spec.channels.count() as u16,
                end: false,
            }));
        }
    }

    fn seek(&mut self, ms: u64) {
        let from = ms.saturating_sub(PRE_ROLL_MS);
        let time = Time::new(from / 1000, (from % 1000) as f64 / 1000.0);
        if self
            .format
            .seek(
                SeekMode::Accurate,
                SeekTo::Time {
                    time,
                    track_id: Some(self.id),
                },
            )
            .is_ok()
        {
            self.decoder.reset();
            self.trim_ms = self.time_base.map(|_| ms);
            self.finished = false;
        }
    }
}

fn resolve(yt: &ytm::Client, urls: &Urls, video_id: &str) -> Result<ytm::Stream, String> {
    if let Some((at, stream)) = urls.lock().unwrap().get(video_id)
        && at.elapsed() < URL_LIFETIME
    {
        return Ok(stream.clone());
    }
    let stream = yt.stream(video_id)?;
    let mut urls = urls.lock().unwrap();
    if urls.len() >= URL_CACHE_ENTRIES {
        urls.clear();
    }
    urls.insert(video_id.to_owned(), (Instant::now(), stream.clone()));
    Ok(stream)
}

struct PcmSource {
    chunks: Chunks,
    samples: Vec<f32>,
    at: usize,
    rate: u32,
    channels: u16,
    shared: Arc<Shared>,
    wake: Wake,
}

impl PcmSource {
    fn new(chunks: Chunks, shared: Arc<Shared>, wake: Wake) -> Self {
        Self {
            chunks,
            samples: vec![0.0; SILENCE_SAMPLES],
            at: 0,
            rate: 44_100,
            channels: 2,
            shared,
            wake,
        }
    }

    fn refill(&mut self) {
        self.at = 0;
        while let Ok(chunk) = self.chunks.lock().unwrap().try_recv() {
            if chunk.epoch != self.shared.epoch.load(Relaxed) {
                continue;
            }
            if chunk.end {
                self.shared.ended.store(true, Relaxed);
                (self.wake)();
                break;
            }
            if chunk.samples.is_empty() {
                continue;
            }
            self.shared.position_ms.store(chunk.start_ms, Relaxed);
            self.rate = chunk.rate;
            self.channels = chunk.channels;
            self.samples = chunk.samples;
            return;
        }
        self.samples.clear();
        self.samples.resize(SILENCE_SAMPLES, 0.0);
    }
}

impl Iterator for PcmSource {
    type Item = f32;

    fn next(&mut self) -> Option<f32> {
        if self.at >= self.samples.len() {
            self.refill();
        }
        let sample = self.samples[self.at];
        self.at += 1;
        Some(sample)
    }
}

impl rodio::Source for PcmSource {
    fn current_span_len(&self) -> Option<usize> {
        Some(self.samples.len())
    }

    fn channels(&self) -> rodio::ChannelCount {
        self.channels
    }

    fn sample_rate(&self) -> rodio::SampleRate {
        self.rate
    }

    fn total_duration(&self) -> Option<Duration> {
        None
    }
}

/// Open the default output, falling back to any stream. Runs on the decode
/// thread, which then owns the output for its whole lifetime.
fn open_output(
    shared: &Arc<Shared>,
    chunks: &Chunks,
    wake: &Wake,
    lost: &Arc<AtomicBool>,
) -> (Option<(rodio::OutputStream, rodio::Sink)>, Option<String>) {
    lost.store(false, Relaxed);
    let flag = lost.clone();
    let notice_loss = move |error| {
        flag.fetch_or(matches!(error, rodio::cpal::StreamError::DeviceNotAvailable), Relaxed);
    };
    let stream = rodio::OutputStreamBuilder::from_default_device()
        .and_then(|device| device.with_error_callback(notice_loss).open_stream_or_fallback())
        .or_else(|_| rodio::OutputStreamBuilder::open_default_stream());
    let output = stream.ok().map(|mut stream| {
        stream.log_on_drop(false);
        let sink = rodio::Sink::connect_new(stream.mixer());
        let volume = f32::from_bits(shared.volume.load(Relaxed));
        sink.set_volume(volume * volume);
        sink.append(PcmSource::new(chunks.clone(), shared.clone(), wake.clone()));
        (stream, sink)
    });
    let device = default_device().filter(|_| output.is_some());
    (output, device)
}

/// Apply the shared pause flag to the loop-owned sink.
fn apply_output(output: &Option<(rodio::OutputStream, rodio::Sink)>, shared: &Arc<Shared>) {
    if let Some((_, sink)) = output.as_ref() {
        if shared.paused.load(Relaxed) {
            sink.pause();
        } else {
            sink.play();
        }
    }
}

fn decode_loop(
    commands: Receiver<Cmd>,
    replies: Sender<Cmd>,
    pcm: SyncSender<Chunk>,
    chunks: Chunks,
    shared: Arc<Shared>,
    yt: Arc<ytm::Client>,
    urls: Urls,
    wake: Wake,
) {
    // The audio output belongs to this thread: opening it here (not on the
    // UI thread, not in shared state) is what lets pause/volume/device work
    // on every OS, including ones where the output types are not Send.
    let lost = Arc::new(AtomicBool::new(false));
    let (mut output, mut device) = open_output(&shared, &chunks, &wake, &lost);
    // A fresh start begins paused, as before.
    shared.paused.store(true, Relaxed);
    apply_output(&output, &shared);
    if output.is_none() {
        *shared.error.lock().unwrap() = Some(NO_OUTPUT.to_owned());
    }
    let mut device_polled = Instant::now();
    let mut track: Option<Track> = None;
    let mut pending: Option<Chunk> = None;
    let mut resume: Option<u64> = None;
    let mut epoch = 0;
    loop {
        if let Some(chunk) = pending.take() {
            match pcm.try_send(chunk) {
                Ok(()) => {}
                Err(TrySendError::Full(chunk)) => pending = Some(chunk),
                Err(TrySendError::Disconnected(_)) => return,
            }
        }
        let decoding = track.as_ref().is_some_and(|t| !t.finished);
        let command = if pending.is_some() {
            let wait = if shared.paused.load(Relaxed) { PAUSED_POLL } else { FULL_POLL };
            match commands.recv_timeout(wait) {
                Ok(command) => Some(command),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => return,
            }
        } else if decoding {
            match commands.try_recv() {
                Ok(command) => Some(command),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Disconnected) => return,
            }
        } else {
            match commands.recv() {
                Ok(command) => Some(command),
                Err(_) => return,
            }
        };
        let current = |generation: u64| shared.generation.load(Relaxed) == generation;
        match command {
            Some(Cmd::Load {
                video_id,
                generation,
                epoch: started,
            }) => {
                (track, pending, resume, epoch) = (None, None, None, started);
                let (replies, shared, yt, urls) = (replies.clone(), shared.clone(), yt.clone(), urls.clone());
                std::thread::spawn(move || {
                    let opened = Track::open(&yt, &urls, &video_id, generation, &shared).or_else(|message| {
                        urls.lock().unwrap().remove(&video_id);
                        if shared.generation.load(Relaxed) == generation && yt.refresh_clients() {
                            Track::open(&yt, &urls, &video_id, generation, &shared)
                        } else {
                            Err(message)
                        }
                    });
                    let reply = match opened {
                        Ok(track) => Cmd::Opened(track),
                        Err(message) => {
                            urls.lock().unwrap().remove(&video_id);
                            Cmd::Failed { generation, message }
                        }
                    };
                    let _ = replies.send(reply);
                });
            }
            Some(Cmd::Opened(mut opened)) if current(opened.generation) => {
                if let Some(ms) = resume.take() {
                    opened.seek(ms);
                }
                track = Some(opened);
                shared.loading.store(false, Relaxed);
                wake();
            }
            Some(Cmd::Failed { generation, message }) if current(generation) => {
                *shared.error.lock().unwrap() = Some(message);
                shared.loading.store(false, Relaxed);
                wake();
            }
            Some(Cmd::Seek { ms, epoch: flushed }) => {
                (pending, epoch) = (None, flushed);
                match &mut track {
                    Some(track) => track.seek(ms),
                    None => resume = Some(ms),
                }
            }
            Some(Cmd::Pause { .. }) => {
                apply_output(&output, &shared);
            }
            Some(Cmd::SetVolume { volume }) => {
                if let Some((_, sink)) = output.as_ref() {
                    sink.set_volume(volume * volume);
                }
            }
            Some(Cmd::PollDevice) => {
                if device_polled.elapsed() >= DEVICE_POLL {
                    device_polled = Instant::now();
                    if default_device() != device || lost.load(Relaxed) {
                        let had_output = output.take().is_some();
                        (output, device) = open_output(&shared, &chunks, &wake, &lost);
                        apply_output(&output, &shared);
                        if had_output && output.is_none() {
                            *shared.error.lock().unwrap() = Some(NO_OUTPUT.to_owned());
                        }
                    }
                }
            }
            Some(Cmd::Preload { video_id }) => {
                let (yt, urls) = (yt.clone(), urls.clone());
                std::thread::spawn(move || resolve(&yt, &urls, &video_id));
            }
            _ => {}
        }
        if pending.is_some() {
            continue;
        }
        let Some(playing) = track.as_mut().filter(|t| !t.finished) else {
            continue;
        };
        let mut advanced = false;
        match playing.next_chunk(epoch) {
            Ok(Some(chunk)) => pending = Some(chunk),
            Ok(None) => {
                playing.finished = true;
                if current(playing.generation) {
                    let video_id = playing.video_id.clone();
                    if shared.queue.lock().unwrap().repeat == 2 {
                        // Repeat-one: restart the same track right here instead
                        // of waiting for a UI frame that may never come.
                        epoch = shared.epoch.fetch_add(1, Relaxed) + 1;
                        pending = None;
                        playing.seek(0);
                        if !playing.finished {
                            shared.position_ms.store(0, Relaxed);
                            shared.ended.store(false, Relaxed);
                            wake();
                        }
                    } else {
                        let next = {
                            let queue = shared.queue.lock().unwrap();
                            let at = queue.items.iter().position(|item| item.video_id == video_id);
                            match at {
                                Some(i) if i + 1 < queue.items.len() => Some(queue.items[i + 1].video_id.clone()),
                                _ if queue.repeat == 1 && !queue.items.is_empty() => Some(queue.items[0].video_id.clone()),
                                _ => None,
                            }
                        };
                        if let Some(next_id) = next {
                            let generation = shared.generation.fetch_add(1, Relaxed) + 1;
                            epoch = shared.epoch.fetch_add(1, Relaxed) + 1;
                            pending = None;
                            resume = None;
                            shared.position_ms.store(0, Relaxed);
                            shared.duration_ms.store(0, Relaxed);
                            shared.buffered.store(0, Relaxed);
                            shared.ended.store(false, Relaxed);
                            shared.loading.store(true, Relaxed);
                            shared.error.lock().unwrap().take();
                            *shared.current_id.lock().unwrap() = next_id.clone();
                            wake();
                            let warm_id = next_id.clone();
                            // The UI thread usually preloads the stream after
                            // that, but it may be stalled in the background.
                            let (warm_yt, warm_urls, warm_shared) = (yt.clone(), urls.clone(), shared.clone());
                            let (replies, shared, yt, urls) = (replies.clone(), shared.clone(), yt.clone(), urls.clone());
                            std::thread::spawn(move || {
                                let opened = Track::open(&yt, &urls, &next_id, generation, &shared).or_else(|message| {
                                    urls.lock().unwrap().remove(&next_id);
                                    if shared.generation.load(Relaxed) == generation && yt.refresh_clients() {
                                        Track::open(&yt, &urls, &next_id, generation, &shared)
                                    } else {
                                        Err(message)
                                    }
                                });
                                let reply = match opened {
                                    Ok(track) => Cmd::Opened(track),
                                    Err(message) => {
                                        urls.lock().unwrap().remove(&next_id);
                                        Cmd::Failed { generation, message }
                                    }
                                };
                                let _ = replies.send(reply);
                            });
                            // The UI thread usually preloads the stream after
                            // that, but it may be stalled in the background.
                            std::thread::spawn(move || {
                                let follow = {
                                    let queue = warm_shared.queue.lock().unwrap();
                                    queue
                                        .items
                                        .iter()
                                        .position(|item| item.video_id == warm_id)
                                        .and_then(|i| queue.items.get(i + 1).map(|item| item.video_id.clone()))
                                };
                                if let Some(id) = follow {
                                    let _ = resolve(&warm_yt, &warm_urls, &id);
                                }
                            });
                            advanced = true;
                        }
                    }
                }
                if !advanced && playing.finished {
                    pending = Some(Chunk {
                        samples: Vec::new(),
                        start_ms: 0,
                        epoch,
                        rate: 0,
                        channels: 0,
                        end: true,
                    });
                }
            }
            Err(message) => {
                if current(playing.generation) {
                    urls.lock().unwrap().remove(&playing.video_id);
                    *shared.error.lock().unwrap() = Some(message);
                    wake();
                }
                track = None;
            }
        }
        if advanced {
            track = None;
        }
    }
}

fn default_device() -> Option<String> {
    use rodio::cpal::traits::{DeviceTrait, HostTrait};
    rodio::cpal::default_host().default_output_device()?.name().ok()
}

pub struct Player {
    commands: Sender<Cmd>,
    shared: Arc<Shared>,
    wake: Wake,
    yt: Arc<ytm::Client>,
    urls: Urls,
}

/// Starts a track: the single entry point whoever is asking (a UI button,
/// an MPRIS command, the decode thread chaining on its own). Bumps the
/// generation so stale opens are ignored, resets the shared state, and
/// hands the open to the decode loop, which runs with or without frames.
fn start_track(commands: &Sender<Cmd>, shared: &Arc<Shared>, video_id: &str) {
    let generation = shared.generation.fetch_add(1, Relaxed) + 1;
    let epoch = shared.epoch.fetch_add(1, Relaxed) + 1;
    shared.position_ms.store(0, Relaxed);
    shared.duration_ms.store(0, Relaxed);
    shared.buffered.store(0, Relaxed);
    shared.ended.store(false, Relaxed);
    shared.loading.store(true, Relaxed);
    shared.error.lock().unwrap().take();
    *shared.current_id.lock().unwrap() = video_id.to_owned();
    let _ = commands.send(Cmd::Load {
        video_id: video_id.to_owned(),
        generation,
        epoch,
    });
}

/// Neighbour of `video_id` in the queue snapshot, honouring repeat-all
/// wraparound. Repeat-one never reaches here (it restarts in place).
/// Returns `None` past the ends when repeat is off.
/// Only used by the Linux MPRIS integration (background next/previous).
#[cfg(all(unix, not(target_os = "macos")))]
fn neighbor(queue: &Queue, video_id: &str, delta: i64) -> Option<String> {
    let at = queue.items.iter().position(|item| item.video_id == video_id)?;
    let len = queue.items.len() as i64;
    let mut next = at as i64 + delta;
    if next < 0 || next >= len {
        if queue.repeat == 1 && len > 0 {
            next = if delta > 0 { 0 } else { len - 1 };
        } else {
            return None;
        }
    }
    queue.items.get(next as usize).map(|item| item.video_id.clone())
}

/// Thread-safe handle to the player for background integrations (the
/// MPRIS handler and ticker) that must keep working while the UI thread
/// is stalled. Reads and commands both go through the shared state and
/// the decode loop, never through frames — the same primitives the UI
/// buttons use, so both callers always agree.
#[derive(Clone)]
pub struct Playback {
    commands: Sender<Cmd>,
    shared: Arc<Shared>,
}

impl Playback {
    /// Linux MPRIS only: position/queue readers below are unused elsewhere.
    #[cfg(all(unix, not(target_os = "macos")))]
    pub fn position_ms(&self) -> u64 {
        self.shared.position_ms.load(Relaxed)
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    pub fn duration_ms(&self) -> u64 {
        self.shared.duration_ms.load(Relaxed)
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    pub fn paused(&self) -> bool {
        self.shared.paused.load(Relaxed)
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    pub fn playing(&self) -> bool {
        !self.chained_id().is_empty() && !self.paused()
    }

    /// Set when the audio of the current track really ran out. Unlike
    /// `take_ended` this only peeks, so the UI thread still gets its own
    /// notification when it wakes up.
    #[cfg(all(unix, not(target_os = "macos")))]
    pub fn ended(&self) -> bool {
        self.shared.ended.load(Relaxed)
    }

    /// Video id the decode thread is currently on (or loading).
    #[cfg(all(unix, not(target_os = "macos")))]
    pub fn chained_id(&self) -> String {
        self.shared.current_id.lock().unwrap().clone()
    }

    /// Queue item behind a video id, if the UI snapshot knows it.
    #[cfg(all(unix, not(target_os = "macos")))]
    pub fn queued_item(&self, video_id: &str) -> Option<ytm::Item> {
        self.shared
            .queue
            .lock()
            .unwrap()
            .items
            .iter()
            .find(|item| item.video_id == video_id)
            .cloned()
    }

    pub fn load(&self, video_id: &str) {
        *self.shared.resume_at.lock().unwrap() = None;
        start_track(&self.commands, &self.shared, video_id);
    }

    pub fn seek_to(&self, ms: u64) {
        let epoch = self.shared.epoch.fetch_add(1, Relaxed) + 1;
        self.shared.position_ms.store(ms, Relaxed);
        self.shared.ended.store(false, Relaxed);
        let _ = self.commands.send(Cmd::Seek { ms, epoch });
    }

    /// Store the flag instantly (UI icons and MPRIS read it) and let the
    /// decode loop apply it to the sink it owns.
    pub fn set_paused(&self, paused: bool) {
        self.shared.paused.store(paused, Relaxed);
        let _ = self.commands.send(Cmd::Pause { paused });
    }

    /// Mirrors the UI toggle: resume a restored session once, otherwise
    /// flip the pause state. No-op when nothing is loaded (the pending
    /// resume is kept for whoever loads first).
    /// Linux MPRIS only: the other platforms go through UI events.
    #[cfg(all(unix, not(target_os = "macos")))]
    pub fn toggle(&self) {
        let id = self.chained_id();
        if id.is_empty() {
            return;
        }
        if let Some(position) = self.take_resume() {
            start_track(&self.commands, &self.shared, &id);
            self.seek_to(position);
        } else {
            self.set_paused(!self.paused());
        }
    }

    /// Mirrors a manual next: step to the following queue item (wrapping
    /// on repeat-all), doing nothing at the end otherwise.
    /// Linux MPRIS only: the other platforms go through UI events.
    #[cfg(all(unix, not(target_os = "macos")))]
    pub fn next(&self) {
        let id = self.chained_id();
        let next = neighbor(&self.shared.queue.lock().unwrap(), &id, 1);
        if let Some(next) = next {
            self.load(&next);
            self.preload_after(&next, 1);
        }
    }

    /// Mirrors a manual previous: restart the track past the restart
    /// threshold, otherwise step back (or restart at the head).
    /// Linux MPRIS only: the other platforms go through UI events.
    #[cfg(all(unix, not(target_os = "macos")))]
    pub fn previous(&self) {
        if self.chained_id().is_empty() {
            return;
        }
        if self.position_ms() >= crate::app::RESTART_AFTER_MS {
            self.seek_to(0);
            return;
        }
        let id = self.chained_id();
        let next = neighbor(&self.shared.queue.lock().unwrap(), &id, -1);
        match next {
            Some(prev) => self.load(&prev),
            None => self.seek_to(0),
        }
    }

    /// Warm the stream cache for the item after `video_id`, like the UI
    /// does when a track changes, so background steps stay gapless.
    /// Linux MPRIS only: the other platforms go through UI events.
    #[cfg(all(unix, not(target_os = "macos")))]
    fn preload_after(&self, video_id: &str, delta: i64) {
        let next = neighbor(&self.shared.queue.lock().unwrap(), video_id, delta);
        if let Some(id) = next {
            let _ = self.commands.send(Cmd::Preload { video_id: id });
        }
    }

    pub fn resume(&self) -> Option<u64> {
        *self.shared.resume_at.lock().unwrap()
    }

    pub fn take_resume(&self) -> Option<u64> {
        self.shared.resume_at.lock().unwrap().take()
    }

    pub fn set_resume(&self, position: Option<u64>) {
        *self.shared.resume_at.lock().unwrap() = position;
    }
}

impl Player {
    pub fn new(yt: Arc<ytm::Client>, wake: impl Fn() + Send + Sync + 'static) -> Self {
        let wake: Wake = Arc::new(wake);
        let shared = Arc::new(Shared {
            volume: AtomicU32::new(1.0f32.to_bits()),
            ..Default::default()
        });
        let urls = Urls::default();
        let (commands, inbox) = mpsc::channel();
        let (pcm, chunks) = mpsc::sync_channel(QUEUED_CHUNKS);
        let chunks = Arc::new(Mutex::new(chunks));
        let worker = (commands.clone(), shared.clone(), yt.clone(), urls.clone(), wake.clone());
        let chunks = chunks.clone();
        std::thread::spawn(move || decode_loop(inbox, worker.0, pcm, chunks, worker.1, worker.2, worker.3, worker.4));
        Self {
            commands,
            shared,
            wake,
            yt,
            urls,
        }
    }

    /// Ask the decode loop (which owns the output) to recheck the default
    /// device. Cheap: the loop throttles to one check per `DEVICE_POLL`.
    pub fn follow_default_device(&self) {
        let _ = self.commands.send(Cmd::PollDevice);
    }

    pub fn load(&self, video_id: &str) {
        self.playback().load(video_id);
        self.set_paused(false);
    }

    pub fn preload(&self, video_id: &str) {
        let (yt, urls, video_id) = (self.yt.clone(), self.urls.clone(), video_id.to_owned());
        std::thread::spawn(move || resolve(&yt, &urls, &video_id));
    }

    /// Shares the UI queue with the decode thread so it can chain to the
    /// next track on its own when the UI thread is stalled (a minimised or
    /// hidden window stops eframe redraws on Wayland). `repeat`: 0 = off,
    /// 1 = repeat all, 2 = repeat one.
    pub fn set_queue(&self, items: &[ytm::Item], repeat: u8) {
        *self.shared.queue.lock().unwrap() = Queue {
            items: items.to_vec(),
            repeat,
        };
    }

    /// Thread-safe handle to the player for background integrations (the
    /// MPRIS handler and ticker) that must keep working while the UI
    /// thread is stalled. Commands go through the same primitives the
    /// UI buttons use.
    pub fn playback(&self) -> Playback {
        Playback {
            commands: self.commands.clone(),
            shared: self.shared.clone(),
        }
    }

    /// Video id the decode thread is currently on (or loading).
    pub fn chained_id(&self) -> String {
        self.shared.current_id.lock().unwrap().clone()
    }

    pub fn seek(&self, ms: u64) {
        self.playback().seek_to(ms);
    }

    pub fn set_paused(&self, paused: bool) {
        self.playback().set_paused(paused);
    }

    pub fn paused(&self) -> bool {
        self.shared.paused.load(Relaxed)
    }

    pub fn set_volume(&self, volume: f32) {
        self.shared.volume.store(volume.to_bits(), Relaxed);
        let _ = self.commands.send(Cmd::SetVolume { volume });
    }

    pub fn position_ms(&self) -> u64 {
        self.shared.position_ms.load(Relaxed)
    }

    pub fn duration_ms(&self) -> u64 {
        self.shared.duration_ms.load(Relaxed)
    }

    pub fn buffered(&self) -> f32 {
        self.shared.buffered.load(Relaxed) as f32 / 1000.0
    }

    pub fn loading(&self) -> bool {
        self.shared.loading.load(Relaxed)
    }

    pub fn take_ended(&self) -> bool {
        self.shared.ended.swap(false, Relaxed)
    }

    pub fn take_error(&self) -> Option<String> {
        self.shared.error.lock().unwrap().take()
    }
}

pub fn selftest(query: &str) -> Result<String, String> {
    let started = Instant::now();
    let yt = ytm::Client::new(String::new());
    let song = yt.search(query)?.songs().into_iter().next().ok_or("search returned no songs")?;
    let searched = started.elapsed();
    let radio = yt.radio(&song.video_id)?;
    let shared = Arc::new(Shared::default());
    let opening = Instant::now();
    let mut track = Track::open(&yt, &Urls::default(), &song.video_id, 0, &shared)?;
    let opened = opening.elapsed();
    let mut frames = 0u64;
    let mut peak = 0f32;
    let mut rate = 0;
    while frames < 3 * 44_100 {
        let chunk = track.next_chunk(0)?.ok_or("stream ended early")?;
        frames += (chunk.samples.len() / chunk.channels.max(1) as usize) as u64;
        peak = chunk.samples.iter().fold(peak, |peak, sample| peak.max(sample.abs()));
        rate = chunk.rate;
    }
    if peak <= 0.001 {
        return Err("decoded audio is silent".to_owned());
    }
    let duration = shared.duration_ms.load(Relaxed);
    let far = if duration > SELFTEST_FAR_SEEK_MS + 60_000 {
        SELFTEST_FAR_SEEK_MS
    } else {
        60_000
    };
    let seeking = Instant::now();
    for target in [60_000, 5_000, far, 5_000] {
        track.seek(target);
        let landed = track.next_chunk(0)?.ok_or("seek ran past the end")?.start_ms;
        if landed.abs_diff(target) > 1000 {
            return Err(format!("seek to {target} ms landed at {landed} ms"));
        }
    }
    Ok(format!(
        "ok: \"{}\" by {} | search {searched:.0?} | open {opened:.0?} | {rate} Hz, peak {peak:.2} | seeks to 60s, 5s, {}s, 5s in {:.0?} | radio {} tracks | duration {duration} ms",
        song.title,
        song.subtitle,
        far / 1000,
        seeking.elapsed(),
        radio.len(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reopened_output_keeps_playing_the_queued_audio() {
        let shared = Arc::new(Shared::default());
        let (pcm, chunks) = mpsc::sync_channel(QUEUED_CHUNKS);
        let chunks: Chunks = Arc::new(Mutex::new(chunks));
        let open = || PcmSource::new(chunks.clone(), shared.clone(), Arc::new(|| {}));
        let queue = |start_ms| {
            let chunk = Chunk {
                samples: vec![0.5; 4],
                start_ms,
                epoch: 0,
                rate: 48_000,
                channels: 2,
                end: false,
            };
            pcm.try_send(chunk).is_ok()
        };
        assert!(queue(1000) && queue(2000));
        let mut first = open();
        assert_eq!(first.by_ref().take(SILENCE_SAMPLES + 4).filter(|sample| *sample == 0.5).count(), 4);
        drop(first);
        let mut second = open();
        assert_eq!(second.by_ref().take(SILENCE_SAMPLES + 4).filter(|sample| *sample == 0.5).count(), 4);
        assert_eq!(shared.position_ms.load(Relaxed), 2000);
        assert!(queue(3000));
    }

    /// Linux MPRIS only: these tests cover the gated background controls.
    #[cfg(all(unix, not(target_os = "macos")))]
    fn queued(ids: &str, repeat: u8) -> Queue {
        Queue {
            items: ids
                .chars()
                .map(|id| ytm::Item {
                    video_id: id.to_string(),
                    ..ytm::Item::default()
                })
                .collect(),
            repeat,
        }
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    fn playback_with(queue: Queue) -> (Playback, mpsc::Receiver<Cmd>) {
        let shared = Arc::new(Shared::default());
        *shared.queue.lock().unwrap() = queue;
        let (commands, inbox) = mpsc::channel();
        let control = Playback { commands, shared };
        (control, inbox)
    }

    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn queue_neighbours_step_without_wrapping_when_repeat_is_off() {
        let queue = queued("abc", 0);
        assert_eq!(neighbor(&queue, "a", 1).as_deref(), Some("b"));
        assert_eq!(neighbor(&queue, "b", 1).as_deref(), Some("c"));
        assert_eq!(neighbor(&queue, "c", 1), None);
        assert_eq!(neighbor(&queue, "c", -1).as_deref(), Some("b"));
        assert_eq!(neighbor(&queue, "a", -1), None);
        assert_eq!(neighbor(&queue, "zzz", 1), None);
    }

    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn queue_neighbours_wrap_around_on_repeat_all() {
        let queue = queued("abc", 1);
        assert_eq!(neighbor(&queue, "c", 1).as_deref(), Some("a"));
        assert_eq!(neighbor(&queue, "a", -1).as_deref(), Some("c"));
    }

    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn next_loads_the_following_item_and_preloads_the_one_after() {
        let (control, inbox) = playback_with(queued("abc", 0));
        *control.shared.current_id.lock().unwrap() = "a".to_owned();
        control.next();
        match inbox.try_recv() {
            Ok(Cmd::Load { video_id, .. }) => assert_eq!(video_id, "b"),
            _ => panic!("expected a load of b"),
        }
        assert_eq!(*control.shared.current_id.lock().unwrap(), "b");
    }

    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn next_stops_at_the_end_when_repeat_is_off() {
        let (control, inbox) = playback_with(queued("abc", 0));
        *control.shared.current_id.lock().unwrap() = "c".to_owned();
        control.next();
        assert!(inbox.try_recv().is_err());
        assert_eq!(*control.shared.current_id.lock().unwrap(), "c");
    }

    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn previous_restarts_the_track_past_the_restart_threshold() {
        let (control, inbox) = playback_with(queued("abc", 0));
        *control.shared.current_id.lock().unwrap() = "b".to_owned();
        control.shared.position_ms.store(crate::app::RESTART_AFTER_MS + 1, Relaxed);
        control.previous();
        match inbox.try_recv() {
            Ok(Cmd::Seek { ms, .. }) => assert_eq!(ms, 0),
            _ => panic!("expected a seek to zero"),
        }
        assert_eq!(*control.shared.current_id.lock().unwrap(), "b");
    }

    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn pause_flips_the_shared_flag_without_frames() {
        let (control, _) = playback_with(queued("", 0));
        control.set_paused(true);
        assert!(control.paused());
        control.toggle();
        assert!(control.paused(), "toggle with nothing loaded stays put");
        *control.shared.current_id.lock().unwrap() = "a".to_owned();
        control.toggle();
        assert!(!control.paused());
    }
}
