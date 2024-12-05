import sys
sys.path.append('../')
import gi
import configparser
gi.require_version('Gst', '1.0')
from gi.repository import GLib, Gst
from ctypes import *
import time
import sys
import math
import json
import pyds
import uuid
import platform
import numpy as np
from common.platform_info import PlatformInfo
from common.bus_call import bus_call
from common.FPS import PERF_DATA
from azure.iot.device import IoTHubDeviceClient, Message

iot_hub_connection_string = "HostName=saptialAnalysis.azure-devices.net;DeviceId=test_cam_2;SharedAccessKey=BLX6KRb/9zvicnRpCdf6d2txakanqgSHg9ekp4g1P8Y="

perf_data = None
MAX_DISPLAY_LEN=64
PGIE_CLASS_ID_PERSON = 0
PGIE_CLASS_ID_BAG = 1
PGIE_CLASS_ID_FACE = 2
MUXER_OUTPUT_WIDTH=1920
MUXER_OUTPUT_HEIGHT=1080
MUXER_BATCH_TIMEOUT_USEC = 33000
TILED_OUTPUT_WIDTH=1280
TILED_OUTPUT_HEIGHT=720
GST_CAPS_FEATURES_NVMM="memory:NVMM"
OSD_PROCESS_MODE= 0
OSD_DISPLAY_TEXT= 1
pgie_classes_str= ["person","bag","face"]

device_client = IoTHubDeviceClient.create_from_connection_string(iot_hub_connection_string)
device_client.connect()

def _helper_iot(device_client,msg):
    try:
        message = Message(msg)
        device_client.send_message(message)
        return 1
    except Exception as e:
        device_client.disconnect()
        return 0

def send_message_to_iothub(msg):
    global device_client
    max_retries = 3  
    for attempt in range(max_retries):
        is_suc_send_msg = _helper_iot(device_client, msg)
        if is_suc_send_msg:
            print('Success: Data sent to IoT Hub:', msg)
            return
        else:
            print(f"Attempt {attempt + 1} failed. Retrying...")
            try:
                device_client = IoTHubDeviceClient.create_from_connection_string(iot_hub_connection_string)
                device_client.connect()
            except Exception as e:
                print(f"Failed to recreate device client: {e}")
                break  
    print("Error: Failed to send message after multiple retries. Saving for later retry.")

def generate_unique_number():
    unique_number = uuid.uuid4().int
    truncated_number = int(str(unique_number)[:12])
    return truncated_number

global unique_number
unique_number = generate_unique_number()

def nvanalytics_src_pad_buffer_probe(pad,info,u_data):
    frame_number=0
    num_rects=0
    gst_buffer = info.get_buffer()
    if not gst_buffer:
        print("Unable to get GstBuffer ")
        return
    batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
    l_frame = batch_meta.frame_meta_list
    while l_frame:
        try:
            frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
            
        except StopIteration:
            break
        frame_number=frame_meta.frame_num
        l_obj=frame_meta.obj_meta_list
        num_rects = frame_meta.num_obj_meta
        obj_counter = {
                            PGIE_CLASS_ID_PERSON : 0,
                            PGIE_CLASS_ID_BAG : 0,
                            PGIE_CLASS_ID_FACE : 0
                        }
        while l_obj:
            try:
                obj_meta=pyds.NvDsObjectMeta.cast(l_obj.data)
            except StopIteration:
                break
            obj_counter[obj_meta.class_id] += 1
            l_user_meta = obj_meta.obj_user_meta_list
            while l_user_meta:
                try:
                    user_meta = pyds.NvDsUserMeta.cast(l_user_meta.data)
 
                    if user_meta.base_meta.meta_type == pyds.nvds_get_user_meta_type("NVIDIA.DSANALYTICSOBJ.USER_META"):            
                        user_meta_data = pyds.NvDsAnalyticsObjInfo.cast(user_meta.user_meta_data)
                except StopIteration:
                    break
                try:
                    l_user_meta = l_user_meta.next
                except StopIteration:
                    break
            try:
                l_obj=l_obj.next
            except StopIteration:
                break
        l_user = frame_meta.frame_user_meta_list
        while l_user:
            try:
                user_meta = pyds.NvDsUserMeta.cast(l_user.data)
                if user_meta.base_meta.meta_type == pyds.nvds_get_user_meta_type("NVIDIA.DSANALYTICSFRAME.USER_META"):
                    user_meta_data = pyds.NvDsAnalyticsFrameMeta.cast(user_meta.user_meta_data)
            except StopIteration:
                break
            try:
                l_user = l_user.next
            except StopIteration:
                break
        try:
            if frame_number % 375 == 0:
                data = json.dumps({
                                    "stream_id": frame_meta.pad_index, 
                                    "unique_number_uuid": unique_number,                   
                                    "batch_id": frame_meta.batch_id,  
                                    "frame_num": frame_meta.frame_num,
                                    "buf_pts": frame_meta.buf_pts, 
                                    "ntp_timestamp": frame_meta.ntp_timestamp, 
                                    "source_id": frame_meta.source_id,
                                    "object_count": user_meta_data.objCnt,
                                    "Number_of_objects":num_rects,
                                    "People_count":obj_counter[PGIE_CLASS_ID_PERSON],
                                    "object_in_ROI_count": user_meta_data.objInROIcnt,
                                    "cumulative_line_cross_count": user_meta_data.objLCCumCnt,
                                    "current_line_cross_count": user_meta_data.objLCCurrCnt,
                                    "overcrowding_status": user_meta_data.ocStatus,
                                    "unique_id": user_meta_data.unique_id
                                    })
                print('-------data----',data)
                send_message_to_iothub(data)
        except Exception as e:
            print(f"Error occurred while sending data to eventhub: {e}")

        print("Frame Number=", frame_number, 
              "stream id=", frame_meta.pad_index, 
              "Number of Objects=",num_rects,
              "People count=",obj_counter[PGIE_CLASS_ID_PERSON],
              "bag_count=",obj_counter[PGIE_CLASS_ID_BAG],
              "Face_count=",obj_counter[PGIE_CLASS_ID_FACE])
        stream_index = "stream{0}".format(frame_meta.pad_index)
        global perf_data
        perf_data.update_fps(stream_index)
        try:
            l_frame=l_frame.next
        except StopIteration:
            break
    return Gst.PadProbeReturn.OK
 
def cb_newpad(decodebin, decoder_src_pad,data):
    print("In cb_newpad\n")
    caps=decoder_src_pad.get_current_caps()
    gststruct=caps.get_structure(0)
    gstname=gststruct.get_name()
    source_bin=data
    features=caps.get_features(0)
    print("gstname=",gstname)
    if(gstname.find("video")!=-1):
        if features.contains("memory:NVMM"):
            bin_ghost_pad=source_bin.get_static_pad("src")
            if not bin_ghost_pad.set_target(decoder_src_pad):
                sys.stderr.write("Failed to link decoder src pad to source bin ghost pad\n")
        else:
            sys.stderr.write(" Error: Decodebin did not pick nvidia decoder plugin.\n")
 
def decodebin_child_added(child_proxy,Object,name,user_data):
    print("Decodebin child added:", name, "\n")
    if(name.find("decodebin") != -1):
        Object.connect("child-added",decodebin_child_added,user_data)
 
def create_source_bin(index,uri):
    print("Creating source bin")
    bin_name="source-bin-%02d" %index
    print(bin_name)
    nbin=Gst.Bin.new(bin_name)
    if not nbin:
        sys.stderr.write(" Unable to create source bin \n")
    uri_decode_bin=Gst.ElementFactory.make("uridecodebin", "uri-decode-bin")
    if not uri_decode_bin:
        sys.stderr.write(" Unable to create uri decode bin \n")
    uri_decode_bin.set_property("uri",uri)
    uri_decode_bin.connect("pad-added",cb_newpad,nbin)
    uri_decode_bin.connect("child-added",decodebin_child_added,nbin)
    Gst.Bin.add(nbin,uri_decode_bin)
    bin_pad=nbin.add_pad(Gst.GhostPad.new_no_target("src",Gst.PadDirection.SRC))
    if not bin_pad:
        sys.stderr.write(" Failed to add ghost pad in source bin \n")
        return None
    return nbin
 
send_data_time_interval = 10
last_saved_time = 0 
def frame_filter_pad_probe(pad, info, u_data):
    global last_saved_time
    current_time = time.time()
    if current_time - last_saved_time >= send_data_time_interval:
        last_saved_time = current_time
        return Gst.PadProbeReturn.OK  
    else:
        return Gst.PadProbeReturn.DROP 

def main(args):

    if len(args) < 2:
        sys.stderr.write("usage: %s <uri1> [uri2] ... [uriN]\n" % args[0])
        sys.exit(1)
 
    global perf_data
    perf_data = PERF_DATA(len(args) - 1)
    number_sources=len(args)-1
 
    platform_info = PlatformInfo()
   
    Gst.init(None)
 
    print("Creating Pipeline \n ")
    pipeline = Gst.Pipeline()
    is_live = False
 
    if not pipeline:
        sys.stderr.write(" Unable to create Pipeline \n")

    print("Creating streamux \n ")
    streammux = Gst.ElementFactory.make("nvstreammux", "Stream-muxer")
    if not streammux:
        sys.stderr.write(" Unable to create NvStreamMux \n")
 
    pipeline.add(streammux)
    for i in range(number_sources):
        print("Creating source_bin ",i," \n ")
        uri_name=args[i+1]
        if uri_name.find("rtsp://") == 0 :
            is_live = True
        source_bin=create_source_bin(i, uri_name)
        if not source_bin:
            sys.stderr.write("Unable to create source bin \n")
        pipeline.add(source_bin)
        padname="sink_%u" %i
        sinkpad= streammux.request_pad_simple(padname)
        if not sinkpad:
            sys.stderr.write("Unable to create sink pad bin \n")
        srcpad=source_bin.get_static_pad("src")
        if not srcpad:
            sys.stderr.write("Unable to create src pad bin \n")
        srcpad.link(sinkpad)
    queue1=Gst.ElementFactory.make("queue","queue1")
    queue2=Gst.ElementFactory.make("queue","queue2")
    queue3=Gst.ElementFactory.make("queue","queue3")
    queue4=Gst.ElementFactory.make("queue","queue4")
    queue5=Gst.ElementFactory.make("queue","queue5")
    queue6=Gst.ElementFactory.make("queue","queue6")
    queue7=Gst.ElementFactory.make("queue","queue7")
    pipeline.add(queue1)
    pipeline.add(queue2)
    pipeline.add(queue3)
    pipeline.add(queue4)
    pipeline.add(queue5)
    pipeline.add(queue6)
    pipeline.add(queue7)
 
    print("Creating Pgie \n ")
    pgie = Gst.ElementFactory.make("nvinfer", "primary-inference")
    if not pgie:
        sys.stderr.write(" Unable to create pgie \n")
 
    print("Creating nvtracker \n ")
    tracker = Gst.ElementFactory.make("nvtracker", "tracker")
    if not tracker:
        sys.stderr.write(" Unable to create tracker \n")
 
    print("Creating nvdsanalytics \n ")
    nvanalytics = Gst.ElementFactory.make("nvdsanalytics", "analytics")
    if not nvanalytics:
        sys.stderr.write(" Unable to create nvanalytics \n")
    nvanalytics.set_property("config-file", "config_nvdsanalytics.txt")
 
    print("Creating tiler \n ")
    tiler=Gst.ElementFactory.make("nvmultistreamtiler", "nvtiler")
    if not tiler:
        sys.stderr.write(" Unable to create tiler \n")
 
    print("Creating nvvidconv \n ")
    nvvidconv = Gst.ElementFactory.make("nvvideoconvert", "convertor")
    if not nvvidconv:
        sys.stderr.write(" Unable to create nvvidconv \n")
 
    print("Creating nvosd \n ")
    nvosd = Gst.ElementFactory.make("nvdsosd", "onscreendisplay")
    if not nvosd:
        sys.stderr.write(" Unable to create nvosd \n")
    nvosd.set_property('process-mode',OSD_PROCESS_MODE)
    nvosd.set_property('display-text',OSD_DISPLAY_TEXT)

    # Display
    # if platform_info.is_integrated_gpu():
    #     print("Creating nv3dsink \n")
    #     sink = Gst.ElementFactory.make("nv3dsink", "nv3d-sink")
    #     if not sink:
    #         sys.stderr.write(" Unable to create nv3dsink \n")
    # else:
    #     if platform_info.is_platform_aarch64():
    #         print("Creating nv3dsink \n")
    #         sink = Gst.ElementFactory.make("nv3dsink", "nv3d-sink")
    #     else:
    #         print("Creating EGLSink \n")
    #         sink = Gst.ElementFactory.make("nveglglessink", "nvvideo-renderer")
    #     if not sink:
    #         sys.stderr.write(" Unable to create egl sink \n")
    # sink.set_property("qos",0)

    # no display
    # print("Creating fakesink \n")
    # sink = Gst.ElementFactory.make("fakesink", "fakesink")
    # if not sink:
    #     sys.stderr.write(" Unable to create fakesink \n")

    # Save Image
    nvvidconv2 = Gst.ElementFactory.make("nvvideoconvert", "convertor2")
    if not nvvidconv:
        sys.stderr.write(" Unable to create nvvidconv \n")

    jpegenc = Gst.ElementFactory.make("nvjpegenc", "jpeg-encoder")
    if not jpegenc:
        sys.stderr.write("Unable to create nvjpegenc \n")

    multifilesink = Gst.ElementFactory.make("multifilesink", "multi-file-sink")
    if not multifilesink:
        sys.stderr.write(" Unable to create multifilesink \n")
    multifilesink.set_property("location", "/Deepstream_output/rtsp_v3/frame-%04d.jpg") 
    multifilesink.set_property("post-messages", True)
 
    if is_live:
        print("Atleast one of the sources is live")
        streammux.set_property('live-source', 1)
 
    streammux.set_property('width', 1920)
    streammux.set_property('height', 1080)
    streammux.set_property('batch-size', number_sources)
    streammux.set_property('batched-push-timeout', MUXER_BATCH_TIMEOUT_USEC)
    pgie.set_property('config-file-path', "dsnvanalytics_pgie_config.txt")
    pgie_batch_size=pgie.get_property("batch-size")
    if(pgie_batch_size != number_sources):
        print("WARNING: Overriding infer-config batch-size",pgie_batch_size," with number of sources ", number_sources," \n")
        pgie.set_property("batch-size",number_sources)

    tiler_rows=int(math.sqrt(number_sources))
    tiler_columns=int(math.ceil((1.0*number_sources)/tiler_rows))
    tiler.set_property("rows",tiler_rows)
    tiler.set_property("columns",tiler_columns)
    tiler.set_property("width", TILED_OUTPUT_WIDTH)
    tiler.set_property("height", TILED_OUTPUT_HEIGHT)
 
    #Set properties of tracker
    config = configparser.ConfigParser()
    config.read('dsnvanalytics_tracker_config.txt')
    config.sections()
 
    for key in config['tracker']:
        if key == 'tracker-width' :
            tracker_width = config.getint('tracker', key)
            tracker.set_property('tracker-width', tracker_width)
        if key == 'tracker-height' :
            tracker_height = config.getint('tracker', key)
            tracker.set_property('tracker-height', tracker_height)
        if key == 'gpu-id' :
            tracker_gpu_id = config.getint('tracker', key)
            tracker.set_property('gpu_id', tracker_gpu_id)
        if key == 'll-lib-file' :
            tracker_ll_lib_file = config.get('tracker', key)
            tracker.set_property('ll-lib-file', tracker_ll_lib_file)
        if key == 'll-config-file' :
            tracker_ll_config_file = config.get('tracker', key)
            tracker.set_property('ll-config-file', tracker_ll_config_file)
 
    print("Adding elements to Pipeline \n")
    pipeline.add(pgie)
    pipeline.add(tracker)
    pipeline.add(nvanalytics)
    pipeline.add(tiler)
    pipeline.add(nvvidconv)
    pipeline.add(nvosd)
    pipeline.add(nvvidconv2)
    pipeline.add(jpegenc)
    pipeline.add(multifilesink)
 
    print("Linking elements in the Pipeline \n")
    streammux.link(queue1)
    queue1.link(pgie)
    pgie.link(queue2)
    queue2.link(tracker)
    tracker.link(queue3)
    queue3.link(nvanalytics)
    nvanalytics.link(queue4)
    queue4.link(tiler)
    tiler.link(queue5)
    queue5.link(nvvidconv)
    nvvidconv.link(queue6)
    queue6.link(nvosd)
    nvosd.link(queue7)
    queue7.link(nvvidconv2)
    nvvidconv2.link(jpegenc)
    jpegenc.link(multifilesink)

    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect ("message", bus_call, loop)
    nvanalytics_src_pad=nvanalytics.get_static_pad("src")

    sinkpad = multifilesink.get_static_pad("sink")
    if not sinkpad:
        sys.stderr.write(" Unable to get sink pad of multifilesink \n")
        sys.exit(1)
    sinkpad.add_probe(Gst.PadProbeType.BUFFER, frame_filter_pad_probe, None)

    if not nvanalytics_src_pad:
        sys.stderr.write(" Unable to get src pad \n")
    else:
        nvanalytics_src_pad.add_probe(Gst.PadProbeType.BUFFER, nvanalytics_src_pad_buffer_probe, 0)
        # perf callback function to print fps every 5 sec
        GLib.timeout_add(5000, perf_data.perf_print_callback)
 
    print("Now playing...")
    for i, source in enumerate(args):
        if (i != 0):
            print(i, ": ", source)
 
    print("Starting pipeline \n") 
    pipeline.set_state(Gst.State.PLAYING)
    try:
        loop.run()
    except:
        pass
    print("Exiting app\n")
    pipeline.set_state(Gst.State.NULL)
 
if __name__ == '__main__':
    sys.exit(main(sys.argv))