import type { SVGProps } from "react";

type IconProps = SVGProps<SVGSVGElement>;

function Icon({ children, ...props }: IconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" {...props}>
      {children}
    </svg>
  );
}

export const UploadIcon = (props: IconProps) => <Icon {...props}><path d="M12 16V4m0 0 4 4m-4-4L8 8"/><path d="M5 14v5h14v-5"/></Icon>;
export const ImageIcon = (props: IconProps) => <Icon {...props}><rect x="3" y="4" width="18" height="16" rx="2"/><circle cx="9" cy="10" r="2"/><path d="m4 17 5-4 4 3 3-2 4 3"/></Icon>;
export const LocateIcon = (props: IconProps) => <Icon {...props}><circle cx="12" cy="12" r="3"/><path d="M12 2v3m0 14v3M2 12h3m14 0h3"/><circle cx="12" cy="12" r="8"/></Icon>;
export const LayersIcon = (props: IconProps) => <Icon {...props}><path d="m12 3-9 5 9 5 9-5-9-5Z"/><path d="m3 12 9 5 9-5M3 16l9 5 9-5"/></Icon>;
export const CopyIcon = (props: IconProps) => <Icon {...props}><rect x="8" y="8" width="11" height="11" rx="2"/><path d="M16 8V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h3"/></Icon>;
export const ExternalIcon = (props: IconProps) => <Icon {...props}><path d="M14 4h6v6m0-6-9 9"/><path d="M20 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V5a1 1 0 0 1 1-1h5"/></Icon>;
export const CloseIcon = (props: IconProps) => <Icon {...props}><path d="m6 6 12 12M18 6 6 18"/></Icon>;
export const ChevronIcon = (props: IconProps) => <Icon {...props}><path d="m8 10 4 4 4-4"/></Icon>;
export const TelegramIcon = (props: IconProps) => <Icon {...props}><path d="m21 4-3 16-6-4-3 3-1-5-5-2 18-8Z"/><path d="m8 14 9-7"/></Icon>;
export const AlertIcon = (props: IconProps) => <Icon {...props}><path d="M12 3 2.8 20h18.4L12 3Z"/><path d="M12 9v4m0 3h.01"/></Icon>;
export const CheckIcon = (props: IconProps) => <Icon {...props}><path d="m5 12 4 4L19 6"/></Icon>;
