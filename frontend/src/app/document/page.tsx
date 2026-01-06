import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import Image from "next/image"

export default function DocumentPage() {
  return (
    <div className="space-y-8">
      <div className="relative w-full max-w-3xl mx-auto">
        <Image
          src="/images/howto.png"
          alt="How to use SpotLake"
          width={0}
          height={0}
          className="w-full h-auto rounded-md"
          style={{ width: '100%', height: 'auto' }}
        />
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-2xl">1. Vendor selection</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-muted-foreground leading-relaxed">
            On the demo page, users can select one cloud vendor among AWS, Google Cloud, or Azure to show the latest spot instance dataset in the table below. The table shows the latest dataset of the selected cloud vendor, and it contains every pair of instance types and regions provided by the vendor.
          </p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-2xl">2. Querying</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-muted-foreground leading-relaxed">
            Since the default table shows only the latest dataset of every instance-region pair, users have to query with specific Instance Type, Region, AZ, and Date Range options to get the historical dataset. Data query has some limitations; the maximum number of the returned data point is 20,000 and user can set the date range up to 1 month. If user selects the &apos;ALL&apos; option in Region or AZ field, the returned dataset contains every Regions or AZs corresponding to the Instance Type option.
          </p>
          <p className="text-muted-foreground leading-relaxed">
            Even if user send query with specific date range, SpotLake does not return data points in the date range. SpotLake system only saves the data point when there is a change in any fields. Therefore, user only get the changed data points with demo page&apos;s querying feature. If you want to get the full dataset, check the &apos;How to access full dataset&apos; section on about page.
          </p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-2xl">3. Filtering</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground leading-relaxed">
            User can apply additional filter to the table that shows default latest dataset or queried dataset. For instance, user can select specific data points that contains specific character in Instance Type column or filter by size of the score. Also table could be exported in the CSV format with EXPORT button.
          </p>
        </CardContent>
      </Card>
    </div>
  )
}
