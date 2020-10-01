//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
#include <iostream>

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_frames) {
  curr_frames = 0;
  this->num_frames = num_frames;
  hand = 0;
  arr.resize(num_frames);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  size_t pinned_pages_cnt = 0;
  for (; pinned_pages_cnt < num_frames; hand = (hand + 1) % num_frames) {
    if (!arr[hand].active) {
      continue;
    }
    if (!arr[hand].pin) {
      if (!arr[hand].ref) {
        *frame_id = hand;
        curr_frames -= 1;
        arr[hand].pin = arr[hand].ref = arr[hand].active = false;
        return true;
      } else {
        arr[hand].ref = false;
      }
    } else {
      pinned_pages_cnt += 1;
    }
  }
  // TODO: check
  *frame_id = 0;
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  if (!arr[frame_id].active) return;
  arr[frame_id].pin = true;
  arr[frame_id].ref = true;
  curr_frames -= 1;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  if (!arr[frame_id].active) {
    curr_frames += 1;
  }
  arr[frame_id].active = true;
  arr[frame_id].pin = false;
  arr[frame_id].ref = true;
}

size_t ClockReplacer::Size() { return curr_frames; }

}  // namespace bustub
