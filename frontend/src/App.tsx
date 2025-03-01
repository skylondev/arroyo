// General react imports.
import { useMemo, useState } from 'react';

// MRT imports.
import {
  MantineReactTable,
  useMantineReactTable,
  type MRT_ColumnDef,
  type MRT_ColumnFilterFnsState,
  type MRT_ColumnFiltersState,
  type MRT_PaginationState,
  type MRT_SortingState,
} from 'mantine-react-table';

// UI elements.
import { ActionIcon, Tooltip, Box, Text, Stack, Group, Table, Flex, Anchor, Container } from '@mantine/core';
import { IconRefresh } from '@tabler/icons-react';

// react-query imports.
import {
  QueryClient,
  QueryClientProvider,
  useQuery,
} from '@tanstack/react-query';

// Charts.
import { LineChart } from '@mantine/charts';
import '@mantine/charts/styles.css';

import './App.css';

// NOTE: single encounter data point, containing a UTC date
// (in string format) and the corresponding distance between
// two objects involved in a conjunction.
type encounter_data_point = {
  date: string;
  dist: number;
};

// Single row in the conjunctions table sent by the backend.
type single_row = {
  norad_ids: string;
  object_names: string;
  norad_id_i: number;
  norad_id_j: number;
  object_name_i: string;
  object_name_j: string;
  ops_status_i: string;
  ops_status_j: string;
  object_id_i: string;
  object_id_j: string;
  launch_date_i: string;
  launch_date_j: string;
  object_type_i: string;
  object_type_j: string;
  rcs_i: number | null;
  rcs_j: number | null;
  tca: string;
  dca: number;
  relative_speed: number;
  tca_diff: number;
  dca_diff: number;
  relative_speed_diff: number;
  // NOTE: this is the data which is visualised only
  // if the row is expanded.
  expanded_data: Array<encounter_data_point>;
};

// Set of rows that will be sent by the backend.
type rows_response = {
  // The conjunctions to be visualised in the current page.
  rows: Array<single_row>,
  // The total number of rows.
  tot_nrows: number;
  // The total number of conjunctions.
  tot_nconj: number;
  // The conjunction threshold.
  threshold: number;
  // The conjunctions timestamp.
  conj_ts: string;
  // The total computation time (in seconds).
  comp_time: number;
  // The number of missed conjunctions.
  n_missed_conj: number;
  // The time period covered by the computation.
  date_begin: string;
  date_end: string;
}

// Bundle of parameters to be passed to useGetConjunctions(). This contains
// several MRT types describing aspects of the table state such as the sorting
// criteria, pagination, filtering, etc.
type useGetConjunctionsParams = {
  columnFilterFns: MRT_ColumnFilterFnsState;
  columnFilters: MRT_ColumnFiltersState;
  sorting: MRT_SortingState;
  pagination: MRT_PaginationState;
}

// Fetch the API URL from the env variable.
const ARROYO_API_URL = import.meta.env.VITE_ARROYO_API_URL;

// react-query hook to fetch the list of conjunctions from the backend.
const useGetConjunctions = ({ columnFilterFns, columnFilters, sorting, pagination }: useGetConjunctionsParams) => {
  // API url.
  const url = `${ARROYO_API_URL}/socrates_comparison/`;

  // The body for the POST request. Here we are setting all the parameters
  // to be passed to the backend.
  const body = {
    begin: pagination.pageIndex * pagination.pageSize,
    nrows: pagination.pageSize,
    sorting: sorting,
    filter_fns: columnFilterFns,
    filters: columnFilters,
  };

  // Define the function that performs the API call.
  const queryFunction = async () => {
    // Options for the request.
    const requestOptions = {
      method: 'POST',
      // NOTE: it is important to set the correct content type here.
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    };

    // Make the request.
    const response = await fetch(url, requestOptions);

    // Fetch the result
    const responseData = await response.json();

    // Handle errors.
    if (!response.ok) {
      // Get error message from body or default to response status.
      const error = (responseData && responseData.message) || response.status;
      return Promise.reject(error);
    }

    return responseData;
  };

  // NOTE: what this does is essentially adding a few features on top of just
  // calling queryFunction() directly. The important bit for us it the caching behaviour:
  // if the result of a previous invocation of queryFunction() for a given 'body' was already
  // computed, the cached result will be returned.
  return useQuery<rows_response>({
    // Here's the cache: we need to give a unique name and pass the current 'body'.
    queryKey: ['socrates_comparison', body],
    queryFn: queryFunction,
    // NOTE: see https://github.com/TanStack/query/discussions/6460.
    placeholderData: (prev) => prev,
    // NOTE: do not refetch previously viewed pages until cache is more than 30 seconds old.
    staleTime: 30_000,
  });
};

// Function to create the table of conjunctions.
const ConjunctionsTable = () => {
  // Manage MRT state that we want to pass to our API.

  // The filter values.
  const [columnFilters, setColumnFilters] = useState<MRT_ColumnFiltersState>(
    [],
  );

  // Filter modes. We have different defaults depending on the column.
  const [columnFilterFns, setColumnFilterFns] =
    useState<MRT_ColumnFilterFnsState>(
      {
        'norad_ids': 'contains', 'object_names': 'contains',
        'dca': 'lessThan', 'relative_speed': 'lessThan',
        'tca_diff': 'lessThan', 'dca_diff': 'lessThan',
        'relative_speed_diff': 'lessThan',
      }
    );

  // Sorting.
  const [sorting, setSorting] = useState<MRT_SortingState>([]);

  // Pagination.
  const [pagination, setPagination] = useState<MRT_PaginationState>({
    pageIndex: 0,
    pageSize: 10,
  });

  // Call the react-query hook to fetch the table data from the backend.
  const { data: table_data, isError: table_data_error, isFetching: table_data_fetching, isLoading: table_data_loading, refetch: table_refetch } = useGetConjunctions({
    columnFilterFns,
    columnFilters,
    pagination,
    sorting,
  });

  // Fetch the conjunctions for the current page and the total
  // number of conjunctions from the response.
  const fetchedConjunctions = table_data?.rows ?? [];
  const totalRowCount = table_data?.tot_nrows ?? 0;

  // Fetch the threshold value.
  const threshold = table_data?.threshold ?? 0;

  // Setup the "missed conjunctions" text element.
  const n_missed_conj = table_data?.n_missed_conj ?? 0;
  const missed_conj = <Text component="span" fw={700} size="l" c={n_missed_conj == 0 ? "green.6" : "red.6"}>
    {n_missed_conj}
  </Text>;

  // Fetch date_begin/date_end.
  const date_begin = table_data?.date_begin ?? "N/A";
  const date_end = table_data?.date_end ?? "N/A";

  // Definition of the columns.
  const columns = useMemo<MRT_ColumnDef<single_row>[]>(
    () => {
      // Allowed predicates for range-based filters.
      const range_filter_modes = ['greaterThan', 'lessThan', 'between', 'betweenInclusive'];

      return [
        {
          accessorKey: 'norad_ids',
          header: 'Norad ID',
          columnFilterModeOptions: ['contains'],
          size: 60,
          enableSorting: false,
          Cell: ({ cell }) => {
            const [first, second] = cell.getValue<string>().split(" | ");

            return <Stack gap="2px"><Box style={(theme) => ({
              backgroundColor:
                theme.colors.blue[9],
              borderRadius: '5px',
              color: '#fff',
              padding: '2px',
            })}>
              <Text size="sm" fw={700}>{first}</Text>
            </Box>
              <Box style={(theme) => ({
                backgroundColor:
                  theme.colors.blue[9],
                borderRadius: '5px',
                color: '#fff',
                padding: '2px',
              })}>
                <Text size="sm" fw={700}>{second}</Text>
              </Box></Stack>;
          },
        },
        {
          accessorKey: 'object_names',
          header: 'Name',
          columnFilterModeOptions: ['contains'],
          enableSorting: false,
          Cell: ({ cell }) => {
            const [first, second] = cell.getValue<string>().split(" | ");

            return <Stack gap="2px"><Box style={(theme) => ({
              backgroundColor:
                theme.colors.indigo[9],
              borderRadius: '5px',
              color: '#fff',
              padding: '2px',
            })}>
              <Text size="sm" fw={700}>{first}</Text>
            </Box>
              <Box style={(theme) => ({
                backgroundColor:
                  theme.colors.indigo[9],
                borderRadius: '5px',
                color: '#fff',
                padding: '2px',
              })}>
                <Text size="sm" fw={700}>{second}</Text>
              </Box></Stack>;
          },
        },
        {
          accessorKey: 'tca',
          header: 'TCA (UTC)',
          enableColumnFilter: false,
        },
        {
          accessorKey: 'dca',
          header: 'DCA (km)',
          columnFilterModeOptions: range_filter_modes,
          Cell: ({ cell }) => (
            <Box
              style={(theme) => ({
                backgroundColor:
                  cell.getValue<number>() < threshold / 10
                    ? theme.colors.red[9]
                    : cell.getValue<number>() >= threshold / 10 &&
                      cell.getValue<number>() < threshold / 2
                      ? theme.colors.yellow[9]
                      : theme.colors.green[9],
                borderRadius: '5px',
                color: '#fff',
                padding: '4px',
              })}
            >
              {cell.getValue<number>().toPrecision(4)}
            </Box>
          ),
        },
        {
          accessorKey: 'relative_speed',
          header: 'Rel. speed (km/s)',
          columnFilterModeOptions: range_filter_modes,
          Cell: ({ cell }) => cell.getValue<number>().toPrecision(4),
        },
        {
          accessorKey: 'tca_diff',
          header: 'ΔTCA (ms)',
          columnFilterModeOptions: range_filter_modes,
          Cell: ({ cell }) => cell.getValue<number>().toPrecision(4),
        },
        {
          accessorKey: 'dca_diff',
          header: 'ΔDCA (m)',
          columnFilterModeOptions: range_filter_modes,
          Cell: ({ cell }) => cell.getValue<number>().toPrecision(4),
        },
        {
          accessorKey: 'relative_speed_diff',
          header: 'ΔRel. speed (m/s)',
          columnFilterModeOptions: range_filter_modes,
          Cell: ({ cell }) => cell.getValue<number>().toPrecision(4),
        },
      ];
    },
    [threshold],
  );

  const table = useMantineReactTable({
    columns,
    data: fetchedConjunctions,
    enableColumnFilterModes: true,
    columnFilterModeOptions: [],
    initialState: { density: 'xs' },
    manualFiltering: true,
    manualPagination: true,
    manualSorting: true,
    mantineToolbarAlertBannerProps: table_data_error
      ? {
        color: 'red',
        children: 'Error loading data',
      }
      : undefined,
    onColumnFilterFnsChange: setColumnFilterFns,
    onColumnFiltersChange: setColumnFilters,
    onPaginationChange: setPagination,
    onSortingChange: setSorting,
    renderTopToolbarCustomActions: () => (
      <Group>
        <Tooltip label="Refresh data">
          <ActionIcon onClick={() => table_refetch()} size='l'>
            <IconRefresh />
          </ActionIcon>
        </Tooltip>
        <Text size="sm">Total conjunctions: <strong>{table_data?.tot_nconj ?? 0}</strong></Text>
        <Text size="sm">|</Text>
        <Text size="sm">Last updated: <strong>{table_data?.conj_ts ?? "N/A"} (UTC)</strong></Text>
        <Text size="sm">|</Text>
        <Text size="sm">Time interval: <strong>{date_begin}</strong> — <strong>{date_end} (UTC)</strong></Text>
        <Text size="sm">|</Text>
        <Text size="sm">Threshold: <strong>{threshold}km</strong></Text>
        <Text size="sm">|</Text>
        <Text size="sm">Runtime: <strong>{(table_data?.comp_time ?? 0).toPrecision(4)}s</strong></Text>
        <Text size="sm">|</Text>
        <Text size="sm">Missed conjunctions: {missed_conj}</Text>
      </Group>
    ),
    rowCount: totalRowCount,
    state: {
      columnFilterFns,
      columnFilters,
      isLoading: table_data_loading,
      pagination,
      showAlertBanner: table_data_error,
      showProgressBars: table_data_fetching,
      sorting,
    },
    enableColumnActions: false,
    enableColumnDragging: false,
    enableColumnOrdering: true,
    enableGlobalFilter: false,
    mantineTableContainerProps: {
      style: {
        maxHeight: '1280px',
      },
    },
    enableStickyHeader: true,
    enableFullScreenToggle: false,
    mantineTableHeadCellProps: {
      align: 'center',
    },
    mantineTableBodyCellProps: {
      align: 'center',
    },
    renderDetailPanel: ({ row }) => {
      return (
        <Container style={{ width: "75%" }}>
          <Flex justify="center" gap="xl">
            <Table variant="vertical" layout="fixed" withTableBorder style={{ flex: 1 }}>
              <Table.Tbody>
                <Table.Tr>
                  <Table.Th>Norad ID</Table.Th>
                  <Table.Td>{row.original.norad_id_i}</Table.Td>
                  <Table.Td>{row.original.norad_id_j}</Table.Td>
                </Table.Tr>
                <Table.Tr>
                  <Table.Th>Name</Table.Th>
                  <Table.Td>{row.original.object_name_i}</Table.Td>
                  <Table.Td>{row.original.object_name_j}</Table.Td>
                </Table.Tr>
                <Table.Tr>
                  <Table.Th><Anchor href="https://celestrak.org/satcat/status.php" target="_blank" fw="inherit" fz="inherit">OPS status</Anchor></Table.Th>
                  <Table.Td>[{row.original.ops_status_i}]</Table.Td>
                  <Table.Td>[{row.original.ops_status_j}]</Table.Td>
                </Table.Tr>
                <Table.Tr>
                  <Table.Th>COSPAR ID</Table.Th>
                  <Table.Td>{row.original.object_id_i}</Table.Td>
                  <Table.Td>{row.original.object_id_j}</Table.Td>
                </Table.Tr>
                <Table.Tr>
                  <Table.Th>Launch date</Table.Th>
                  <Table.Td>{row.original.launch_date_i}</Table.Td>
                  <Table.Td>{row.original.launch_date_j}</Table.Td>
                </Table.Tr>
                <Table.Tr>
                  <Table.Th><Anchor href="https://celestrak.org/satcat/satcat-format.php" target="_blank" fw="inherit" fz="inherit">Object type</Anchor></Table.Th>
                  <Table.Td>{row.original.object_type_i}</Table.Td>
                  <Table.Td>{row.original.object_type_j}</Table.Td>
                </Table.Tr>
                <Table.Tr>
                  <Table.Th>RCS (m²)</Table.Th>
                  <Table.Td>{row.original.rcs_i?.toString() ?? "N/A"}</Table.Td>
                  <Table.Td>{row.original.rcs_j?.toString() ?? "N/A"}</Table.Td>
                </Table.Tr>
              </Table.Tbody>
            </Table>
            <LineChart
              style={{ flex: 1 }}
              data={row.original.expanded_data}
              dataKey="date"
              unit="km"
              series={[
                { name: 'dist', color: 'indigo.5', label: 'Range' },
              ]}
              curveType="natural"
              withDots={false}
              xAxisProps={{
                label: {
                  value: "Time",
                  position: "insideBottom",
                  offset: 5,
                  fontSize: 12,
                },
                tick: false
              }}
              yAxisProps={{
                label: {
                  value: "Range",
                  position: "insideLeft",
                  offset: -5,
                  fontSize: 12,
                  angle: -90,
                  domain: [0, "auto"]
                },
              }}
              strokeWidth={3}
              referenceLines={[
                { y: threshold, label: `${threshold} km`, color: 'orange.6', strokeDasharray: 2 },
              ]}
            />
          </Flex>
        </Container>
      );
    },
  });

  return <MantineReactTable table={table} />;
};

const queryClient = new QueryClient();

const ConjunctionsQueryProvider = () => (
  <QueryClientProvider client={queryClient}>
    <ConjunctionsTable />
  </QueryClientProvider>
);

export default ConjunctionsQueryProvider;
