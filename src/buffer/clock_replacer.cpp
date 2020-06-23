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

#include <algorithm>
#include <iostream>


namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) : max_pages_(num_pages) {
    clock_.reserve(num_pages);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) { 
    if (clock_.empty()) {
        frame_id = nullptr;
        return false;
    }

    size_t start_pos = hand_idx_;
    while (true) {
        if (clock_[hand_idx_].second == false) {
            *frame_id = clock_[hand_idx_].first;
            // std::cout << "EVICTIING: " <<  clock_[hand_idx_].first << std::endl;
            clock_.erase(clock_.begin() + hand_idx_);
            return true;
        } else {
            clock_[hand_idx_].second = false;
        }
        
        hand_idx_ = (hand_idx_ + 1) % clock_.size();
        

        // if we go back to where we started
        if (hand_idx_ == start_pos) {
            *frame_id = clock_[hand_idx_].first;
            clock_.erase(clock_.begin() + hand_idx_);
            return true;
        }
    }


    return false; 

}

void ClockReplacer::Pin(frame_id_t frame_id) {

    std::lock_guard<std::mutex> lock(mutex_);
    auto found_frame = FindFrame(frame_id);
    
    if (found_frame != clock_.end() ) {
        // edge case: clock hand points to last element in array, must move back 1
        if (clock_[hand_idx_].first == frame_id && hand_idx_ == clock_.size() - 1)
            hand_idx_ -= 1;
        clock_.erase(found_frame);
    }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found_frame = FindFrame(frame_id);
    // ignore unpins when max size reached
    // if frame DNE add to clock, otherwise, just set ref
    if (found_frame == clock_.end() && clock_.size() < max_pages_) {
      clock_.emplace_back(frame_id, true);
    } else {
        found_frame->second = true;
    }
}

size_t ClockReplacer::Size() { 
  return clock_.size();
}


std::vector<std::pair<frame_id_t, bool>>::iterator ClockReplacer::FindFrame(frame_id_t frame_id) {
    auto found_frame = std::find_if(clock_.begin(), clock_.end(), 
    [&](const auto& p1){
        return p1.first == frame_id;    
    });   
    return found_frame;
}

}  // namespace bustub
