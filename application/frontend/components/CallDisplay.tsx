'use client';

import type { PitchCall } from '@/lib/types';

interface CallDisplayProps {
  call: PitchCall;
}

export default function CallDisplay({ call }: CallDisplayProps) {
  const locationText =
    'zone' in call.location
      ? call.location.zone
      : `(${call.location.x.toFixed(2)}, ${call.location.y.toFixed(2)})`;

  // Determine confidence color
  const getConfidenceColor = (confidence: number) => {
    if (confidence >= 80) return 'bg-green-500';
    if (confidence >= 65) return 'bg-yellow-500';
    return 'bg-orange-500';
  };

  return (
    <div className="w-full rounded-lg border border-gray-200 bg-white p-6 shadow-sm">
      <div className="space-y-4">
        <div>
          <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wide">
            Pitch Call
          </h2>
          <div className="mt-2">
            <div className="text-4xl font-bold text-gray-900">
              {call.pitchType}
            </div>
          </div>
        </div>

        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <p className="text-sm text-gray-500">Location</p>
            <p className="text-lg font-semibold text-gray-900">{locationText}</p>
          </div>

          <div className="flex items-center gap-3">
            <div className="flex-1">
              <div className="flex items-center justify-between text-sm">
                <span className="text-gray-500">Confidence</span>
                <span className="font-semibold text-gray-900">
                  {call.confidence}%
                </span>
              </div>
              <div className="mt-2 h-2 w-full overflow-hidden rounded-full bg-gray-200">
                <div
                  className={`h-full ${getConfidenceColor(call.confidence)} transition-all duration-300`}
                  style={{ width: `${call.confidence}%` }}
                />
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
