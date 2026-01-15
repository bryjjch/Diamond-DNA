'use client';

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getMockPitchCallData } from '@/lib/mockData';
import type { PitchCallData, FeedbackType } from '@/lib/types';

// Fetch pitch call data (mock for now, replace with API call later)
const fetchPitchCall = async (): Promise<PitchCallData> => {
  // Simulate API delay
  await new Promise((resolve) => setTimeout(resolve, 300));
  return getMockPitchCallData();
};

// Submit feedback (mock for now, replace with API call later)
const submitFeedback = async (feedback: FeedbackType): Promise<void> => {
  // Simulate API call
  await new Promise((resolve) => setTimeout(resolve, 500));
  console.log('Feedback submitted:', feedback);
  // In a real implementation, this would call your API
  // await fetch('/api/feedback', { method: 'POST', body: JSON.stringify({ feedback }) });
};

export const usePitchCall = () => {
  return useQuery({
    queryKey: ['pitchCall'],
    queryFn: fetchPitchCall,
    staleTime: 1000 * 60 * 5, // 5 minutes
    refetchOnWindowFocus: false,
  });
};

export const useFeedbackSubmission = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: submitFeedback,
    onSuccess: () => {
      // Refetch pitch call data after feedback submission
      queryClient.invalidateQueries({ queryKey: ['pitchCall'] });
    },
  });
};
