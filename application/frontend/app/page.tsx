'use client';

import { usePitchCall, useFeedbackSubmission } from '@/hooks/usePitchCall';
import CallDisplay from '@/components/CallDisplay';
import StrikeZone from '@/components/StrikeZone';
import GameStateHeader from '@/components/GameStateHeader';
import FeedbackLoop from '@/components/FeedbackLoop';

export default function Home() {
  const { data, isLoading, error, refetch } = usePitchCall();
  const feedbackMutation = useFeedbackSubmission();

  const handleFeedback = async (feedback: string) => {
    try {
      await feedbackMutation.mutateAsync(feedback as any);
      // Refetch new pitch call after feedback
      await refetch();
    } catch (error) {
      console.error('Error submitting feedback:', error);
    }
  };

  if (isLoading) {
  return (
      <div className="flex min-h-screen items-center justify-center bg-gray-50">
        <div className="text-center">
          <div className="mb-4 inline-block h-8 w-8 animate-spin rounded-full border-4 border-solid border-gray-300 border-r-gray-900"></div>
          <p className="text-gray-600">Loading pitch call...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-gray-50">
        <div className="text-center">
          <p className="mb-4 text-red-600">Error loading pitch call</p>
          <button
            onClick={() => refetch()}
            className="rounded bg-blue-500 px-4 py-2 text-white hover:bg-blue-600"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!data) {
    return null;
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Game State Header */}
      <GameStateHeader gameState={data.gameState} />

      {/* Main Content */}
      <main className="mx-auto max-w-6xl space-y-6 p-4 sm:p-6 lg:p-8">
        {/* Call Display */}
        <CallDisplay call={data.call} />

        {/* Strike Zone */}
        <StrikeZone strikeZone={data.strikeZone} />

        {/* Feedback Loop */}
        <FeedbackLoop
          onSubmit={handleFeedback}
          isSubmitting={feedbackMutation.isPending}
        />
      </main>
    </div>
  );
}
