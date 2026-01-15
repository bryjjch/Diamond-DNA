'use client';

import type { FeedbackType } from '@/lib/types';

interface FeedbackLoopProps {
  onSubmit: (feedback: FeedbackType) => void;
  isSubmitting?: boolean;
}

const feedbackOptions: Array<{ type: FeedbackType; label: string; color: string }> = [
  { type: 'ball', label: 'Ball', color: 'bg-blue-500 hover:bg-blue-600' },
  { type: 'called_strike', label: 'Called Strike', color: 'bg-green-500 hover:bg-green-600' },
  { type: 'swinging_strike', label: 'Swinging Strike', color: 'bg-emerald-500 hover:bg-emerald-600' },
  { type: 'foul', label: 'Foul', color: 'bg-yellow-500 hover:bg-yellow-600' },
  { type: 'in_play_out', label: 'In Play (Out)', color: 'bg-orange-500 hover:bg-orange-600' },
  { type: 'in_play_hit', label: 'In Play (Hit)', color: 'bg-red-500 hover:bg-red-600' },
];

export default function FeedbackLoop({ onSubmit, isSubmitting = false }: FeedbackLoopProps) {
  return (
    <div className="w-full rounded-lg border border-gray-200 bg-white p-6 shadow-sm">
      <h2 className="mb-4 text-center text-lg font-semibold text-gray-900">
        What happened?
      </h2>
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        {feedbackOptions.map((option) => (
          <button
            key={option.type}
            onClick={() => onSubmit(option.type)}
            disabled={isSubmitting}
            className={`
              ${option.color}
              min-h-[44px]
              rounded-lg
              px-4
              py-3
              text-sm
              font-semibold
              text-white
              transition-all
              duration-200
              active:scale-95
              disabled:cursor-not-allowed
              disabled:opacity-50
              disabled:active:scale-100
            `}
          >
            {option.label}
          </button>
        ))}
      </div>
    </div>
  );
}
