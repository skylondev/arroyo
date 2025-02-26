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
import { ActionIcon, Tooltip, Box, Text, Stack, Group } from '@mantine/core';
import { IconRefresh } from '@tabler/icons-react';

// react-query imports.
import {
  QueryClient,
  QueryClientProvider,
  useQuery,
} from '@tanstack/react-query';

import './App.css'

// Conjunction datatype.
type Conjunction = {
  conj_index: number;
  norad_ids: string;
  object_names: string;
  tca: string;
  dca: number;
  relative_speed: number;
  tca_diff: number;
  dca_diff: number;
  relative_speed_diff: number;
};

// Type expected from the conjunctions endpoint in the backend.
type ConjunctionApiResponse = {
  // The list of conjunctions to be visualised in the current page.
  rows: Array<Conjunction>,
  // The total number of rows.
  tot_nrows: number;
  // The total number of conjunctions.
  tot_nconj: number;
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

// react-query hook to fetch the list of conjunctions from the backend.
const useGetConjunctions = ({ columnFilterFns, columnFilters, sorting, pagination }: useGetConjunctionsParams) => {
  // API url.
  const url = 'http://localhost:8000/socrates_comparison/'

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

    return responseData as ConjunctionApiResponse;
  };

  // NOTE: what this does is essentially adding a few features on top of just
  // calling queryFunction() directly. The important bit for us it the caching behaviour:
  // if the result of a previous invocation of queryFunction() for a given 'body' was already
  // computed, the cached result will be returned.
  return useQuery<ConjunctionApiResponse>({
    // Here's the cache: we need to give a unique name ('conjunctions')
    // and pass the current 'body'.
    queryKey: ['conjunctions', body],
    queryFn: queryFunction,
    // NOTE: see https://github.com/TanStack/query/discussions/6460.
    placeholderData: (prev) => prev,
    // NOTE: do not refetch previously viewed pages until cache is more than 30 seconds old.
    staleTime: 30_000,
  });
};

// Function to create the table of conjunctions.
const ConjunctionsTable = () => {
  // Allowed predicates for range-based filters.
  const range_filter_modes = ['greaterThan', 'lessThan', 'between', 'betweenInclusive'];

  // Definition of the columns.
  const columns = useMemo<MRT_ColumnDef<Conjunction>[]>(
    () => [
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
            </Box></Stack>
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
            </Box></Stack>
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
                cell.getValue<number>() < 0.5
                  ? theme.colors.red[9]
                  : cell.getValue<number>() >= 0.5 &&
                    cell.getValue<number>() < 2.5
                    ? theme.colors.yellow[9]
                    : theme.colors.green[9],
              borderRadius: '5px',
              color: '#fff',
              padding: '4px',
            })}
          >
            {cell.getValue<Number>().toPrecision(4)}
          </Box>
        ),
      },
      {
        accessorKey: 'relative_speed',
        header: 'Rel. speed (km/s)',
        columnFilterModeOptions: range_filter_modes,
        Cell: ({ cell }) => cell.getValue<Number>().toPrecision(4),
      },
      {
        accessorKey: 'tca_diff',
        header: 'ΔTCA (ms)',
        columnFilterModeOptions: range_filter_modes,
        Cell: ({ cell }) => cell.getValue<Number>().toPrecision(4),
      },
      {
        accessorKey: 'dca_diff',
        header: 'ΔDCA (m)',
        columnFilterModeOptions: range_filter_modes,
        Cell: ({ cell }) => cell.getValue<Number>().toPrecision(4),
      },
      {
        accessorKey: 'relative_speed_diff',
        header: 'ΔRel. speed (m/s)',
        columnFilterModeOptions: range_filter_modes,
        Cell: ({ cell }) => cell.getValue<Number>().toPrecision(4),
      },
    ],
    [],
  );

  // Manage MRT state that we want to pass to our API.
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
  const [sorting, setSorting] = useState<MRT_SortingState>([]);
  const [pagination, setPagination] = useState<MRT_PaginationState>({
    pageIndex: 0,
    pageSize: 10,
  });

  // Call our custom react-query hook to fetch the data from the backend.
  const { data, isError, isFetching, isLoading, refetch } = useGetConjunctions({
    columnFilterFns,
    columnFilters,
    pagination,
    sorting,
  });

  // Fetch the conjunctions for the current page and the total
  // number of conjunctions from the response.
  const fetchedConjunctions = data?.rows ?? [];
  const totalRowCount = data?.tot_nrows ?? 0;

  // Setup the "missed conjunctions" text element.
  const n_missed_conj = data?.n_missed_conj ?? 0;
  const missed_conj = <Text component="span" fw={700} size="l" c={n_missed_conj == 0 ? "green.6" : "red.6"}>
    {n_missed_conj}
  </Text>;

  // Fetch date_begin/date_end.
  const date_begin = data?.date_begin ?? "N/A";
  const date_end = data?.date_end ?? "N/A";

  const table = useMantineReactTable({
    columns,
    data: fetchedConjunctions,
    enableColumnFilterModes: true,
    columnFilterModeOptions: [],
    initialState: { density: 'xs' },
    manualFiltering: true,
    manualPagination: true,
    manualSorting: true,
    mantineToolbarAlertBannerProps: isError
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
        <Tooltip label="Refresh Data">
          <ActionIcon onClick={() => refetch()} size='l'>
            <IconRefresh />
          </ActionIcon>
        </Tooltip>
        <Text size="sm">Total conjunctions: <strong>{data?.tot_nconj ?? 0}</strong></Text>
        <Text size="sm">|</Text>
        <Text size="sm">Last updated: <strong>{data?.conj_ts ?? "N/A"} (UTC)</strong></Text>
        <Text size="sm">|</Text>
        <Text size="sm">Time interval: <strong>{date_begin} (UTC)</strong> — <strong>{date_end} (UTC)</strong></Text>
        <Text size="sm">|</Text>
        <Text size="sm">Runtime: <strong>{(data?.comp_time ?? 0).toPrecision(4)}s</strong></Text>
        <Text size="sm">|</Text>
        <Text size="sm">Missed conjunctions: {missed_conj}</Text>
      </Group>
    ),
    rowCount: totalRowCount,
    state: {
      columnFilterFns,
      columnFilters,
      isLoading,
      pagination,
      showAlertBanner: isError,
      showProgressBars: isFetching,
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
