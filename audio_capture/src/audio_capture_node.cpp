#include <gst/app/gstappsink.h>
#include <gst/gst.h>
#include <stdio.h>

#include <audio_common_msgs/msg/audio_data.hpp>
#include <audio_common_msgs/msg/audio_data_stamped.hpp>
#include <audio_common_msgs/msg/audio_info.hpp>
#include <chrono>
#include <exception>
#include <rclcpp/rclcpp.hpp>
#include <rclcpp_components/register_node_macro.hpp>
#include <thread>

#include "logging.hpp"

#define CHECK(X, Y)               \
  if (!X) {                       \
    LOG_ERROR(Y);                 \
    throw(std::runtime_error(Y)); \
  }

namespace audio_capture
{
static GstFlowReturn s_onNewBuffer(GstAppSink *, gpointer);
static gboolean s_onError(GstBus *, GstMessage *, gpointer);

class AudioCaptureNode : public rclcpp::Node
{
public:
  using AudioData = audio_common_msgs::msg::AudioData;
  using AudioDataStamped = audio_common_msgs::msg::AudioDataStamped;
  using AudioInfo = audio_common_msgs::msg::AudioInfo;
  AudioCaptureNode(const rclcpp::NodeOptions & options) : Node("audio_capture_node", options)
  {
    gst_init(nullptr, nullptr);
    try {
      readAudioParameters();
      initializeROS();
      gst_thread_ = std::thread(&AudioCaptureNode::run, this);
    } catch (const std::runtime_error & e) {
      LOG_ERROR("got runtime error: " << e.what());
    }
  }

  ~AudioCaptureNode()
  {
    LOG_INFO("calling destructor");
    keep_running_ = false;
    shutDown();
    gst_thread_.join();
  }

  void readAudioParameters()
  {
    // source format
    sample_format_ = declare_parameter<std::string>("sample_format", "S24LE");
    // output format
    coding_format_ = declare_parameter<std::string>("coding_format", "mp3");
    // frame id to put on the message
    frame_id_ = declare_parameter<std::string>("frame_id", "audio");
    // The bitrate at which to encode the audio
    bitrate_ = declare_parameter<int>("bitrate", 192);
    // only available for raw data
    channels_ = declare_parameter<int>("channels", 1);
    depth_ = declare_parameter<int>("depth", 16);
    sample_rate_ = declare_parameter<int>("sample_rate", 16000);
    // the source of the audio
    device_ = declare_parameter<std::string>("device", "");
    // max buffers for audio sink
    sink_max_buffers_ = declare_parameter<int>("sink_max_buffers", 100);
    output_file_ = declare_parameter<std::string>("output_file", "");
    source_type_ = declare_parameter<std::string>("source", "alsasrc");
    LOG_INFO("device:        " << device_);
    LOG_INFO("source:        " << source_type_);
    LOG_INFO("sample format: " << sample_format_);
    LOG_INFO("sample rate:   " << sample_rate_);
    LOG_INFO("channels:      " << channels_);
    LOG_INFO("depth:         " << depth_);
    LOG_INFO("enc format:    " << coding_format_);
    LOG_INFO("enc bit rate:  " << bitrate_);
  }

  void initializeROS()
  {
    info_msg_.channels = channels_;
    info_msg_.sample_rate = sample_rate_;
    info_msg_.sample_format = sample_format_;
    info_msg_.bitrate = bitrate_;
    info_msg_.coding_format = coding_format_;

    retry_sleep_ = declare_parameter<int>("retry_sleep_seconds", 5);
    pub_ = create_publisher<AudioData>("~/audio", 10);
    pub_stamped_ = create_publisher<AudioDataStamped>("~/audio_stamped", 10);
    pub_info_ = create_publisher<AudioInfo>("~/audio_info", 1);
    timer_info_ =
      rclcpp::create_timer(this, get_clock(), std::chrono::seconds(5), [this] { publishInfo(); });
    publishInfo();
  }

  // main worker thread
  void run()
  {
    LOG_INFO("gst thread started.");
    while (keep_running_) {
      initGst();
      g_main_loop_run(loop_);  // only returns on error
      LOG_WARN("encountered audio device issue!");
      shutDown();
      if (keep_running_) {
        LOG_INFO("sleeping " << retry_sleep_ << "s before retry");
        std::this_thread::sleep_for(std::chrono::seconds(retry_sleep_));
      }
    }
    LOG_INFO("gst thread exited.");
  }

  void shutDown()
  {
    if (is_running_) {
      is_running_ = false;
      LOG_INFO("shutting down gst state.");
      g_main_loop_quit(loop_);  // should be a no-op
      gst_element_set_state(pipeline_, GST_STATE_NULL);
      gst_object_unref(pipeline_);
      g_main_loop_unref(loop_);  // will this bring down the whole pipeline?
    }
  }

  gboolean initializeForMP3()
  {
    GstCaps * caps = gst_caps_new_simple(
      "audio/x-raw", "format", G_TYPE_STRING, sample_format_.c_str(), "channels", G_TYPE_INT,
      channels_, "width", G_TYPE_INT, depth_, "depth", G_TYPE_INT, depth_, "rate", G_TYPE_INT,
      sample_rate_, "signed", G_TYPE_BOOLEAN, TRUE, NULL);

    filter_ = gst_element_factory_make("capsfilter", "filter");
    CHECK(filter_, "cannot create capsfilter!");
    g_object_set(G_OBJECT(filter_), "caps", caps, NULL);
    gst_caps_unref(caps);
    convert_ = gst_element_factory_make("audioconvert", "convert");
    CHECK(convert_, "cannot create audioconvert element!");
    encode_ = gst_element_factory_make("lamemp3enc", "encoder");
    CHECK(encode_, "cannot create lame_mp3 encoder!");
    g_object_set(G_OBJECT(encode_), "target", 1, NULL);
    g_object_set(G_OBJECT(encode_), "bitrate", bitrate_, NULL);
    gst_bin_add_many(GST_BIN(pipeline_), source_, filter_, convert_, encode_, sink_, NULL);
    const auto status = gst_element_link_many(source_, filter_, convert_, encode_, sink_, NULL);
    LOG_INFO("encoding in mp3 format.");
    return (status);
  }

  gboolean initializeForWave()
  {
    gboolean link_ok;
    if (output_file_.empty()) {
      GstCaps * caps = gst_caps_new_simple(
        "audio/x-raw", "format", G_TYPE_STRING, sample_format_.c_str(), "channels", G_TYPE_INT,
        channels_, "width", G_TYPE_INT, depth_, "depth", G_TYPE_INT, depth_, "rate", G_TYPE_INT,
        sample_rate_, "signed", G_TYPE_BOOLEAN, TRUE, NULL);
      g_object_set(G_OBJECT(sink_), "caps", caps, NULL);
      gst_caps_unref(caps);
      gst_bin_add_many(GST_BIN(pipeline_), source_, sink_, NULL);
      link_ok = gst_element_link_many(source_, sink_, NULL);
      LOG_INFO("encoding in wave format.");
    } else {
      filter_ = gst_element_factory_make("wavenc", "filter");
      gst_bin_add_many(GST_BIN(pipeline_), source_, filter_, sink_, NULL);
      link_ok = gst_element_link_many(source_, filter_, sink_, NULL);
      LOG_INFO("encoding to file in wave format.");
    }
    return (link_ok);
  }

  void setClock()
  {
    GstClock * clock = gst_system_clock_obtain();
    CHECK(clock, "cannot get clock!");
    g_object_set(clock, "clock-type", GST_CLOCK_TYPE_REALTIME, NULL);
    gst_pipeline_use_clock(GST_PIPELINE_CAST(pipeline_), clock);
    gst_object_unref(clock);
    LOG_INFO("clock set to real time.");
  }

  void makeSink()
  {
    if (output_file_.empty()) {
      sink_ = gst_element_factory_make("appsink", "sink");
      CHECK(sink_, "cannot create appsink!");
      g_object_set(G_OBJECT(sink_), "emit-signals", true, NULL);
      g_object_set(G_OBJECT(sink_), "max-buffers", sink_max_buffers_, NULL);
      g_signal_connect(G_OBJECT(sink_), "new-sample", G_CALLBACK(s_onNewBuffer), this);
      LOG_INFO("created appsink for regular capture.");
    } else {
      LOG_INFO("using file sink: " << output_file_);
      sink_ = gst_element_factory_make("filesink", "sink");
      CHECK(sink_, "cannot create filesink!");
      g_object_set(G_OBJECT(sink_), "location", output_file_.c_str(), NULL);
      LOG_INFO("created sink to file: " << output_file_);
    }
  }

  void setupErrorCallbacks()
  {
    GstBus * bus = gst_pipeline_get_bus(GST_PIPELINE(pipeline_));
    CHECK(bus, "cannot get gst bus!");
    gst_bus_add_signal_watch(bus);
    g_signal_connect(bus, "message::error", G_CALLBACK(s_onError), this);
    g_object_unref(bus);
  }

  void initGst()
  {
    loop_ = g_main_loop_new(NULL, false);
    pipeline_ = gst_pipeline_new("ros_pipeline");
    CHECK(pipeline_, "cannot create gst pipeline");

    is_running_ = true;  // got far enough already to require shutdown;

    setupErrorCallbacks();
    setClock();
    makeSink();

    source_ = gst_element_factory_make(source_type_.c_str(), "source");
    CHECK(source_, "cannot create sound source of type: " + source_type_);
    // if no device_ is given, dev will not be set and gst will use default dev
    if (!device_.empty()) {
      g_object_set(G_OBJECT(source_), "device", device_.c_str(), NULL);
    }

    gboolean link_ok{false};

    if (coding_format_ == "mp3") {
      link_ok = initializeForMP3();
    } else if (coding_format_ == "wave") {
      link_ok = initializeForWave();
    } else {
      CHECK(false, "invalid coding format: " + coding_format_);
    }
    CHECK(link_ok, "cannot link GST pipeline!");

    gst_element_set_state(GST_ELEMENT(pipeline_), GST_STATE_PLAYING);
    LOG_INFO("gstreamer initializion complete.");
  }

  void publishInfo()
  {
    if (pub_info_->get_subscription_count() != 0) {
      auto msg = std::make_unique<AudioInfo>(info_msg_);
      pub_info_->publish(std::move(msg));
    }
  }

  GstFlowReturn onNewBuffer(GstAppSink * appsink)
  {
    GstSample * sample;
    g_signal_emit_by_name(appsink, "pull-sample", &sample);
    if (pub_->get_subscription_count() != 0 || pub_stamped_->get_subscription_count() != 0) {
      GstMapInfo map;
      GstBuffer * buffer = gst_sample_get_buffer(sample);
      gst_buffer_map(buffer, &map, GST_MAP_READ);
      if (pub_->get_subscription_count() != 0) {
        auto msg = std::make_unique<AudioData>();
        msg->data.resize(map.size);
        memcpy(msg->data.data(), map.data, map.size);
        pub_->publish(std::move(msg));
      }
      if (pub_stamped_->get_subscription_count() != 0) {
        auto msg = std::make_unique<AudioDataStamped>();
        const GstClockTime buffer_time =
          gst_element_get_base_time(source_) + GST_BUFFER_PTS(buffer);
        msg->header.stamp.sec = RCL_NS_TO_S(buffer_time);
        msg->header.stamp.nanosec = buffer_time - RCL_S_TO_NS(msg->header.stamp.sec);
        msg->header.frame_id = frame_id_;
        msg->audio.data.resize(map.size);
        memcpy(msg->audio.data.data(), map.data, map.size);
        pub_stamped_->publish(std::move(msg));
      }
      gst_buffer_unmap(buffer, &map);
    }
    gst_sample_unref(sample);
    return GST_FLOW_OK;
  }

  gboolean onError(GstMessage * message)
  {
    GError * err;
    gchar * debug;
    gst_message_parse_error(message, &err, &debug);
    LOG_ERROR(err->message);
    shutDown();
    exit(1);
    return FALSE;
  }

private:
  // ROS and control related variables
  rclcpp::Publisher<AudioData>::SharedPtr pub_;
  rclcpp::Publisher<AudioDataStamped>::SharedPtr pub_stamped_;
  rclcpp::Publisher<AudioInfo>::SharedPtr pub_info_;
  rclcpp::TimerBase::SharedPtr timer_info_;
  AudioInfo info_msg_;

  std::string frame_id_;
  int retry_sleep_{0};
  bool is_running_{false};
  bool keep_running_{true};
  std::string output_file_;

  // GST related variables
  std::thread gst_thread_;
  GMainLoop * loop_{nullptr};
  GstElement * pipeline_{nullptr};
  GstElement * source_{nullptr};
  GstElement * filter_{nullptr};
  GstElement * sink_{nullptr};
  GstElement * convert_{nullptr};
  GstElement * encode_{nullptr};

  // audio parameters
  int bitrate_{0};
  int channels_{0};
  int depth_{0};
  int sample_rate_{0};
  int sink_max_buffers_{0};
  std::string coding_format_;
  std::string sample_format_;
  std::string source_type_;
  std::string device_;
};

// static helper functions for callbacks
static GstFlowReturn s_onNewBuffer(GstAppSink * appsink, gpointer userData)
{
  return ((reinterpret_cast<AudioCaptureNode *>(userData))->onNewBuffer(appsink));
}
static gboolean s_onError(GstBus *, GstMessage * message, gpointer userData)
{
  return ((reinterpret_cast<AudioCaptureNode *>(userData))->onError(message));
}

}  // namespace audio_capture

RCLCPP_COMPONENTS_REGISTER_NODE(audio_capture::AudioCaptureNode)
