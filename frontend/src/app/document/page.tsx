import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import Image from "next/image"

export default function DocumentPage() {
  return (
    <div className="space-y-8">
      <div className="relative w-full max-w-3xl mx-auto">
        <Image
          src="/images/howto.jpg"
          alt="How to use SpotLake"
          width={1568}
          height={744}
          className="w-full h-auto rounded-md border"
          priority
        />
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-2xl">1. Vendor selection</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-muted-foreground leading-relaxed">
            Pick one of the three cards at the top — Amazon Web Services, Google
            Cloud Platform, or Microsoft Azure — to show that vendor&apos;s spot
            instance dataset in the table below. The table contains every pair of
            instance types and regions provided by that vendor, together with
            availability, interruption-free score, and pricing. The row count and
            the time the snapshot was collected are shown just above the filters.
          </p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-2xl">2. Filtering</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-muted-foreground leading-relaxed">
            Narrow the table down with the Instance Type, Region, and
            Availability Zone selectors. Each one lets you search and pick as
            many values as you like — selecting nothing means no restriction, so
            there is no separate option for everything. The list only contains
            values that actually appear in the current dataset, and choosing a
            region narrows the instance type and availability zone lists to what
            exists there. Google Cloud has no availability zone field, so that
            selector is hidden for it.
          </p>
          <p className="text-muted-foreground leading-relaxed">
            Filtering happens in your browser, so results update immediately and
            no query is sent to the server. The row count above the filters shows
            how many rows are left out of the whole dataset. Press Reset to clear
            every selector at once.
          </p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-2xl">3. Working with the table</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-muted-foreground leading-relaxed">
            Beyond the selectors above, each column header has its own filter and
            can be sorted. For instance, you can keep only the rows whose
            instance type contains a certain string, or sort by availability
            score to find the most stable instances. Rows where a vendor does not
            report a value are shown as N/A and are always sorted to the bottom.
          </p>
          <p className="text-muted-foreground leading-relaxed">
            Use the Export CSV button to download what the table currently shows,
            including the filters you applied.
          </p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-2xl">4. Data coverage</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-muted-foreground leading-relaxed">
            This page shows a single snapshot, published once a day. The time the
            snapshot was collected is shown as Last updated above the filters, in
            UTC. Because every row comes from the same snapshot, the table has no
            date column.
          </p>
          <p className="text-muted-foreground leading-relaxed">
            Historical data is not served from this page. If you need past
            datasets for research, use the Request Full Dataset button in the top
            right of the site. Access is granted for academic and non-commercial
            research, and the request form states the terms you agree to.
          </p>
        </CardContent>
      </Card>
    </div>
  )
}
