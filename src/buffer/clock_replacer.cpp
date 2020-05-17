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

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  clk_ptr = 0;                         /* initialize the clk_ptr, points to the first place */
  buffer_size = num_pages;             /* The buffer size is the same number as num_pages */
  reflag = vector<bool>(num_pages, 0); /* all the frames are not referred */
  inflag = vector<bool>(num_pages, 0); /* all the frames are not in the ClockReplacer */
}

ClockReplacer::~ClockReplacer() = default;

/*
 * Starting from the current position of clock hand, find the first frame that is
 * both in the `ClockReplacer` and with its ref flag set to false. If a frame is in
 * the `ClockReplacer`, but its ref flag is set to true, change it to false instead.
 * This should be the only method that updates the clock hand.
 */
bool ClockReplacer::Victim(frame_id_t *frame_id) {
  bool ret = false; /* have NOT find the result in the beginning */
  short candi = -1; /* which frame to victim */

  for (size_t i = 0; i < buffer_size; i++) {
    short idx = (clk_ptr + i) % buffer_size;

    /* IF find the first frame that is both in the `ClockReplacer`
     * and with its ref flag set to false */
    if (inflag[idx] && !reflag[idx]) {
      clk_ptr = (idx + 1) % buffer_size; /* increase the clk_ptr by 1 */
      *frame_id = idx;                   /* update the frame_id to the idx */
      inflag[idx] = false;               /* new frame is not in the ClockReplacer */
      ret = true;                        /* found the victim, break immediately */
      break;
    }
    /* ELSE IF a frame is in the `ClockReplacer`,
     * but its ref flag is set to true, change it to false instead */
    else if (inflag[idx] && reflag[idx]) {
      reflag[idx] = false; /* change ref flag to false, victim later */
      /* choose another candi to victim */
      candi = (candi == -1) ? idx : candi; /* IF there's no candidate, the first idx is to victim */
    }
  }

  /* IF have NOT found a result without the ref flag set to false */
  if (!ret) {
    /* IF there's NO candidates */
    if (candi == -1) {
      frame_id = nullptr;
    }
    /* ELSE there's one candidate with the ref flag set to true */
    else {
      clk_ptr = (candi + 1) % buffer_size; /* update the clk_ptr */
      *frame_id = candi;
      inflag[candi] = false; /* the new frame is NOT in the ClockReplacer */
      ret = true;            /* find a candidate, so change the result to true */
    }
  }

  return ret;
}

/*
 * This method should be called after a page is pinned to a frame in the BufferPoolManager.
 * It should remove the frame containing the pinned page from the ClockReplacer.
 */
void ClockReplacer::Pin(frame_id_t frame_id) {
  /* IF frame_id is invalid */
  if (frame_id >= buffer_size || frame_id < 0) return;

  /* remove the frame containing the pinned page from the ClockReplacer */
  inflag[frame_id] = false;
  return;
}

/*
 * This method should be called when the pin_count of a page becomes 0. This method should add
 * the frame containing the unpinned page to the ClockReplacer.
 */
void ClockReplacer::Unpin(frame_id_t frame_id) {
  /* IF frame_id is invalid */
  if (frame_id >= buffer_size || frame_id < 0) return;

  /* add the frame containing the unpinned page to the ClockReplacer */
  inflag[frame_id] = true;
  reflag[frame_id] = true;
  return;
}

/* returns the number of frames that are currently in the ClockReplacer. */
size_t ClockReplacer::Size() {
  size_t counter = 0;

  for (auto i_flag : inflag) {
    /* IF in the ClockReplacer, then take it into account */
    if (i_flag) counter++;
  }

  return counter;
}

}  // namespace bustub
