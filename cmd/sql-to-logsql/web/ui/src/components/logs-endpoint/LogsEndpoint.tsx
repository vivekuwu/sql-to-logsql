import {
  Card, CardAction,
  CardContent, CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card.tsx";
import { Input } from "@/components/ui/input.tsx";
import { Label } from "@/components/ui/label.tsx";
import {Switch} from "@/components/ui/switch.tsx";

export interface LogsEndpointProps {
  readonly endpointUrl?: string;
  readonly bearerToken?: string;
  readonly onUrlChange?: (url: string) => void;
  readonly onTokenChange?: (password: string) => void;
  readonly isLoading?: boolean;
  readonly endpointReadOnly?: boolean;
  readonly endpointEnabled?: boolean;
  readonly onEndpointEnabledChange?: (enabled: boolean) => void;
}

export function LogsEndpoint({
  endpointUrl,
  onUrlChange,
  bearerToken,
  onTokenChange,
  isLoading,
  endpointReadOnly,
  endpointEnabled,
  onEndpointEnabledChange,
}: LogsEndpointProps) {
  return (
    <Card className={"w-full"}>
      <CardHeader>
        <CardTitle>VictoriaLogs endpoint</CardTitle>
        {!endpointReadOnly &&
          <CardDescription>
              You can query data from VictoriaLogs instance or just translate SQL to LogsQL without querying
          </CardDescription>
        }
        {!endpointReadOnly &&
          <CardAction className={"flex flex-row gap-2"}>
            <Switch
                checked={endpointEnabled}
                id={"endpointEnabled"}
                onCheckedChange={onEndpointEnabledChange}
                disabled={isLoading || endpointReadOnly}
                hidden={endpointReadOnly}
                className={"cursor-pointer"}
            />
            <Label htmlFor={"endpointEnabled"} className={"cursor-pointer overflow-hidden text-ellipsis"}>
                Query data
            </Label>
          </CardAction>
        }
      </CardHeader>
      <CardContent className={"flex max-sm:flex-col gap-2"}>
        <div className={"flex flex-col gap-1 sm:w-3/4"}>
          <Label htmlFor={endpointUrl}>URL:</Label>
          <Input
            disabled={isLoading || endpointReadOnly || !endpointEnabled}
            id={"endpointUrl"}
            value={endpointUrl}
            type={"url"}
            placeholder={"https://play-vmlogs.victoriametrics.com"}
            onChange={(e) => onUrlChange && onUrlChange(e.target.value)}
          />
        </div>
        <div className={"flex flex-col gap-1 sm:w-1/4"}>
          <Label htmlFor={"bearerToken"}>Bearer token:</Label>
          <Input
            disabled={isLoading || endpointReadOnly || !endpointEnabled}
            id={"bearerToken"}
            value={bearerToken}
            type={"password"}
            onChange={(e) => onTokenChange && onTokenChange(e.target.value)}
          />
        </div>
      </CardContent>
    </Card>
  );
}
